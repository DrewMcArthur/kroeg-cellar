use std::marker::PhantomData;

use futures::lock::Mutex;
use postgres_protocol::message::{frontend, backend};

use crate::{FrontendReceiver, make_err, types, BoundStatement};
use crate::types::{AnyError, PostgresMessage};

pub struct Statement<'frontend> {
    name: String,
    phantom: PhantomData<Mutex<Box<dyn PostgresMessage + 'frontend>>>,
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
