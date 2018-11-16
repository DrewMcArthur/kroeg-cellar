use futures::{future, stream, Future, Stream};
use tokio_postgres::{error::Error, Client, Statement};

pub struct Statements {
    pub upsert_attributes: Statement,
    pub select_attributes: Statement,
    pub select_quad: Statement,
    pub insert_quads: Statement,
    pub delete_quads: Statement,
}

const STATEMENTS: &[&'static str] = &[
    // upsert_attributes
    "insert into attribute (url) select unwrap($1) on conflict do update set url = excluded.url returning id, url",

    // select_attributes
    "select id, url from attribute where id = any($1)",

    // select_quad
    "select id, quad_id, subject_id, predicate_id, attribute_id, object, type_id, language from quad where quad_id = $1",

    // insert_quads
    "insert into quad (quad_id, subject_id, predicate_id, attribute_id, object, type_id, language) select unwrap($1), unwrap($2), unwrap($3), unwrap($4), unwrap($5), unwrap($6), unwrap($7)",

    // delete_quads
    "delete from quad where quad_id = $1",
];

impl Statements {
    pub fn make(conn: Client) -> impl Future<Item = (Statements, Client), Error = (Error, Client)> {
        stream::iter_ok(STATEMENTS)
            .fold((conn, vec![]), |(mut conn, mut statements), item| {
                conn.prepare(item).then(move |res| match res {
                    Ok(stmt) => {
                        statements.push(stmt);
                        future::ok((conn, statements))
                    }

                    Err(e) => future::err((e, conn)),
                })
            })
            .map(|(conn, mut stmts)| {
                (
                    Statements {
                        upsert_attributes: stmts.remove(0),
                        select_attributes: stmts.remove(0),
                        select_quad: stmts.remove(0),
                        insert_quads: stmts.remove(0),
                        delete_quads: stmts.remove(0),
                    },
                    conn,
                )
            })
    }
}
