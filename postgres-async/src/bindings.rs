use futures::lock::{MutexGuard};
use postgres_protocol::message::backend;

use crate::{Statement, FrontendReceiver, make_err};
use crate::types::{AnyError, PostgresMessage, Row};

#[allow(dead_code)]
pub struct BoundStatement<'frontend: 'stmt, 'stmt> {
    pub statement: &'stmt Statement<'frontend>,
    pub portal: String,
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

        postgres_protocol::message::frontend::execute(&self.portal, 0, &mut buf)?;
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
            postgres_protocol::message::frontend::sync(&mut buf);

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
