use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use std::fmt::Write;

mod bindings;
mod connect;
mod frontend;
mod statement;
pub mod types;

pub use bindings::{BoundQuery, BoundStatement};
pub use connect::{connect, Authentication, Connection};
pub use frontend::{Frontend, FrontendReceiver};
pub use statement::Statement;

fn make_err(errs: backend::ErrorFields) -> String {
    let mut err = String::new();
    for field in errs.iterator() {
        let field = field.unwrap();
        let _ = write!(&mut err, "{:?} ", field.value());
    }

    return err;
}
