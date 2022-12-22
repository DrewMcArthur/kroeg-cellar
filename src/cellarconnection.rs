use async_std::net::TcpStream;
use postgres_async::types::AnyError;
use postgres_async::Connection;

use crate::statements::Statements;

/// A connection to a Kroeg PostgreSQL-backed database.
pub struct CellarConnection {
    pub connection: Connection<'static>,
    pub statements: Statements<'static>,
}

impl CellarConnection {
    /// Connects to a given postgres database over TCP, with no TLS.
    pub async fn connect(
        address: &str,
        username: &str,
        pass: &str,
        db: &str,
    ) -> Result<CellarConnection, AnyError> {
        let stream = TcpStream::connect(address).await?;

        let connection =
            postgres_async::connect(stream, db.to_owned(), username.to_owned(), pass.to_owned())
                .await?;
        let statements = Statements::make(&connection).await?;

        Ok(CellarConnection {
            connection,
            statements,
        })
    }
}
