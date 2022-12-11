use postgres_protocol::message::backend;
use std::collections::HashMap;

use crate::types::AnyError;
use crate::{make_err, Authentication};

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

            NoticeResponse(_) => Ok(false),

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
