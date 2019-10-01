use postgres_protocol::message::{backend, frontend};
use std::collections::HashMap;

use crate::make_err;

pub type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

pub struct Initialization {
    pub key_data: Option<(i32, i32)>,
    pub parameters: HashMap<String, String>,
}

impl Initialization {
    pub fn on_message(
        &mut self,
        message: backend::Message,
        _: &mut Vec<u8>,
    ) -> Result<bool, AnyError> {
        use backend::Message::*;

        match message {
            BackendKeyData(data) => {
                self.key_data = Some((data.process_id(), data.secret_key()));

                Ok(false)
            }

            ParameterStatus(data) => {
                self.parameters
                    .insert(data.name()?.to_owned(), data.value()?.to_owned());

                Ok(false)
            }

            ErrorResponse(data) => Err(make_err(data.fields()).into()),

            NoticeResponse(_) => {
                Ok(false)
            }

            ReadyForQuery(_) => Ok(true),

            _ => Err("unexpected message at this time".into()),
        }
    }
}

pub enum InitializationState {
    Authenticating(Authentication),
    Initializing(Initialization),
}

impl InitializationState {
    pub fn on_message(
        &mut self,
        message: backend::Message,
        buf: &mut Vec<u8>,
    ) -> Result<bool, AnyError> {
        match self {
            InitializationState::Authenticating(auth) => {
                if auth.on_message(message, buf)? {
                    *self = InitializationState::Initializing(Initialization {
                        key_data: None,
                        parameters: HashMap::new(),
                    });

                    Ok(false)
                } else {
                    Ok(false)
                }
            }

            InitializationState::Initializing(init) => init.on_message(message, buf),
        }
    }
}
