use postgres_protocol::message::{backend, frontend};

use crate::make_err;
use crate::types::AnyError;

pub struct Authentication {
    pub username: String,
    pub password: String,
}

impl Authentication {
    pub fn on_message(
        &mut self,
        message: backend::Message,
        buf: &mut Vec<u8>,
    ) -> Result<bool, AnyError> {
        use backend::Message::*;

        match message {
            AuthenticationOk => Ok(true),

            AuthenticationKerberosV5 => Err("unsupported authentication method".into()),
            AuthenticationCleartextPassword => {
                frontend::password_message(self.password.as_bytes(), buf)?;

                Ok(false)
            }

            AuthenticationMd5Password(body) => {
                let hash = postgres_protocol::authentication::md5_hash(
                    self.username.as_bytes(),
                    self.password.as_bytes(),
                    body.salt(),
                );

                frontend::password_message(hash.as_bytes(), buf)?;

                Ok(false)
            }

            AuthenticationScmCredential => Err("unsupported authentication method".into()),
            AuthenticationGssContinue(_) => Err("unsupported authentication method".into()),
            AuthenticationSspi => Err("unsupported authentication method".into()),

            AuthenticationSasl(_) | AuthenticationSaslContinue(_) | AuthenticationSaslFinal(_) => {
                Err("unsupported authentication method".into())
            }

            ErrorResponse(data) => Err(make_err(data.fields()).into()),

            _ => Err("unexpected message at this time".into()),
        }
    }
}
