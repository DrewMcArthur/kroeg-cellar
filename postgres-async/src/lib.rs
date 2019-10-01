use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::lock::{Mutex, MutexGuard};
use futures::sink::SinkExt;
use postgres_protocol::message::{backend, frontend};
use std::fmt::Write;
use std::marker::PhantomData;

pub mod connect;
pub mod types;

pub type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct Frontend<T: Send + Sync> {
    stream: T,
    buf: BytesMut,
    to_send: Vec<u8>,
    notify_channel: Option<mpsc::UnboundedSender<backend::Message>>,
    counter: usize,
}

#[async_trait::async_trait]
pub trait PostgresMessage: Send + Sync {
    fn register_next(&mut self, msg: &[u8]);
    fn generate_name(&mut self) -> String;

    async fn read_message(&mut self) -> Result<backend::Message, AnyError>;
    async fn write_data(&mut self, buf: &[u8]) -> Result<(), AnyError>;
}

#[async_trait::async_trait]
impl<T: Send + Sync + AsyncRead + AsyncWrite + Unpin> PostgresMessage for Frontend<T> {
    fn register_next(&mut self, msg: &[u8]) {
        self.to_send.extend_from_slice(msg);
    }

    fn generate_name(&mut self) -> String {
        self.counter += 1;

        format!("s{}s", self.counter)
    }

    async fn read_message(&mut self) -> Result<backend::Message, AnyError> {
        loop {
            if let Some(msg) = backend::Message::parse(&mut self.buf)? {
                if let backend::Message::NotificationResponse(_) = msg {
                    if let Some(ref mut chan) = self.notify_channel {
                        chan.send(msg).await?;
                        continue;
                    }
                }

                return Ok(msg);
            }

            let mut buffer = [0; 1024];
            let len = self.stream.read(&mut buffer[..]).await?;

            self.buf.extend(&buffer[..len]);
        }
    }

    async fn write_data(&mut self, buf: &[u8]) -> Result<(), AnyError> {
        if !self.to_send.is_empty() {
            self.stream.write_all(&self.to_send).await?;
        }

        self.stream.write_all(buf).await?;

        Ok(())
    }
}

fn make_err(errs: backend::ErrorFields) -> String {
    let mut err = String::new();
    for field in errs.iterator() {
        let field = field.unwrap();
        let _ = write!(&mut err, "{:?} ", field.value());
    }

    return err;
}

/// Anything that SQL commands can be run on.
pub trait FrontendReceiver<'frontend>: Send + Sync {
    fn connection(&self) -> &Mutex<Box<dyn PostgresMessage + 'frontend>>;
}

/// A connection.
pub struct Connection<'frontend> {
    conn: Mutex<Box<dyn PostgresMessage + 'frontend>>,
}

impl<'frontend> FrontendReceiver<'frontend> for Connection<'frontend> {
    fn connection(&self) -> &Mutex<Box<dyn PostgresMessage + 'frontend>> {
        &self.conn
    }
}

pub struct Statement<'frontend> {
    name: String,
    phantom: PhantomData<Mutex<Box<dyn PostgresMessage + 'frontend>>>,
}

#[allow(dead_code)]
pub struct BoundStatement<'frontend: 'stmt, 'stmt> {
    statement: &'stmt Statement<'frontend>,
    portal: String,
}

impl<'frontend> Statement<'frontend> {
    pub async fn parse(
        conn: &impl FrontendReceiver<'frontend>,
        query: &str,
    ) -> Result<Statement<'frontend>, AnyError> {
        let mut guard = conn.connection().lock().await;
        let name = guard.generate_name();

        let mut buf = Vec::new();
        frontend::parse(&name, query, None, &mut buf)?;
        buf.extend_from_slice(b"H\x00\x00\x00\x04");

        guard.write_data(&buf).await?;
        loop {
            let msg = guard.read_message().await?;

            match msg {
                backend::Message::ParseComplete => {
                    return Ok(Statement {
                        name,
                        phantom: PhantomData,
                    })
                }

                backend::Message::ErrorResponse(err) => {
                    return Err(make_err(err.fields()).into());
                }

                _ => return Err("should not occur here".into()),
            }
        }
    }

    pub async fn bind<'stmt>(
        &'stmt self,
        conn: &impl FrontendReceiver<'frontend>,
        params: &[&dyn types::Serializable],
    ) -> Result<BoundStatement<'frontend, 'stmt>, AnyError> {
        use std::iter::{once, repeat};
        let mut guard = conn.connection().lock().await;
        let name = guard.generate_name();

        let mut buf = Vec::new();
        let _ = frontend::bind(
            &name,
            &self.name,
            repeat(1).take(params.len()),
            params,
            |val, buf| Ok(val.serialize(buf)),
            once(1),
            &mut buf,
        );
        buf.extend_from_slice(b"H\x00\x00\x00\x04");

        guard.write_data(&buf).await?;
        loop {
            let msg = guard.read_message().await?;

            match msg {
                backend::Message::BindComplete => {
                    return Ok(BoundStatement {
                        statement: &self,
                        portal: name,
                    })
                }
                backend::Message::ErrorResponse(err) => {
                    return Err(make_err(err.fields()).into());
                }

                _ => return Err("should not occur here".into()),
            }
        }
    }
}

impl<'frontend: 'stmt, 'stmt> BoundStatement<'frontend, 'stmt> {
    pub async fn execute<'bound, 'conn>(
        &'bound mut self,
        conn: &'conn impl FrontendReceiver<'frontend>,
    ) -> Result<BoundQuery<'bound, 'conn, 'stmt, 'frontend>, AnyError>
    where
        'frontend: 'conn + 'bound + 'stmt,
    {
        let mut guard = conn.connection().lock().await;
        let mut buf = Vec::new();

        frontend::execute(&self.portal, 0, &mut buf)?;
        buf.extend_from_slice(b"H\x00\x00\x00\x04");
        guard.write_data(&buf).await?;

        Ok(BoundQuery {
            guard,
            statement: self,
        })
    }
}

#[allow(dead_code)]
pub struct BoundQuery<'bound, 'conn, 'stmt, 'frontend: 'conn + 'bound + 'stmt> {
    guard: MutexGuard<'conn, Box<dyn PostgresMessage + 'frontend>>,
    statement: &'bound mut BoundStatement<'frontend, 'stmt>,
}

pub struct Row(backend::DataRowBody);
impl Row {
    pub fn get<T: types::Deserializable>(&self, index: usize) -> Result<Option<T>, AnyError> {
        match self.0.ranges().nth(index)?.unwrap() {
            Some(range) => T::deserialize(&self.0.buffer()[range]).map(Some),
            None => Ok(None),
        }
    }
}

impl<'bound, 'conn, 'stmt, 'frontend: 'conn + 'bound + 'stmt>
    BoundQuery<'bound, 'conn, 'stmt, 'frontend>
{
    pub async fn next(&mut self) -> Option<Result<Row, AnyError>> {
        loop {
            match self.guard.read_message().await {
                Ok(backend::Message::DataRow(row)) => return Some(Ok(Row(row))),
                Ok(backend::Message::ErrorResponse(err)) => {
                    return Some(Err(make_err(err.fields()).into()));
                }

                Ok(backend::Message::EmptyQueryResponse) => (),
                Ok(backend::Message::PortalSuspended) => (),
                Ok(backend::Message::CommandComplete(_)) => (),
                Ok(_) => continue,

                Err(e) => return Some(Err(e)),
            };

            let mut buf = Vec::new();
            frontend::sync(&mut buf);

            match self.guard.write_data(&buf).await {
                Ok(_) => (),
                Err(e) => return Some(Err(e)),
            }

            loop {
                match self.guard.read_message().await {
                    Ok(backend::Message::ReadyForQuery(_)) => return None,
                    Ok(_) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
        }
    }
}

pub async fn connect<'a, T: 'a + Send + Sync + AsyncRead + AsyncWrite + Unpin>(
    stream: T,
    database: String,
    username: String,
    password: String,
) -> Result<Connection<'a>, AnyError> {
    let mut conn = Frontend {
        stream,
        buf: BytesMut::with_capacity(1024),
        notify_channel: None,
        to_send: Vec::new(),
        counter: 0,
    };

    let mut buf = Vec::new();
    frontend::startup_message(
        vec![("user", &username as &str), ("database", &database)],
        &mut buf,
    )?;

    let mut init = connect::InitializationState::Authenticating(connect::Authentication {
        username,
        password,
    });
    loop {
        if !buf.is_empty() {
            conn.write_data(&buf).await?;
            buf.drain(..);
        }

        let message = conn.read_message().await?;
        if init.on_message(message, &mut buf)? {
            break;
        }
    }

    let boxed = Mutex::new(Box::new(conn) as _);

    Ok(Connection { conn: boxed })
}
