use bytes::BytesMut;
use futures::{channel::mpsc, lock::Mutex, AsyncRead, AsyncWrite, SinkExt, AsyncWriteExt, AsyncReadExt};
use postgres_protocol::message::backend;

use crate::types::{AnyError, PostgresMessage};

/// Anything that SQL commands can be run on.
pub trait FrontendReceiver<'frontend>: Send + Sync {
    fn connection(&self) -> &Mutex<Box<dyn PostgresMessage + 'frontend>>;
}

pub struct Frontend<T: Send + Sync> {
    pub stream: T,
    pub buf: BytesMut,
    pub to_send: Vec<u8>,
    pub notify_channel: Option<mpsc::UnboundedSender<backend::Message>>,
    pub counter: usize,
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
