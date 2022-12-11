use bytes::BytesMut;
use futures::{lock::Mutex, AsyncRead, AsyncWrite};

mod authentication;
pub use authentication::*;

mod initialization;
pub use initialization::*;

use crate::frontend::{Frontend, FrontendReceiver};
use crate::types::{AnyError, PostgresMessage};

/// A connection.
pub struct Connection<'frontend> {
    conn: Mutex<Box<dyn PostgresMessage + 'frontend>>,
}

impl<'frontend> FrontendReceiver<'frontend> for Connection<'frontend> {
    fn connection(&self) -> &Mutex<Box<dyn PostgresMessage + 'frontend>> {
        &self.conn
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
    postgres_protocol::message::frontend::startup_message(
        vec![("user", &username as &str), ("database", &database)],
        &mut buf,
    )?;

    let mut init = InitializationState::Authenticating(Authentication { username, password });
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
