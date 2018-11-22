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
    "insert into attribute (url) select unnest($1::text[]) on conflict (url) do update set url = excluded.url returning id, url",

    // select_attributes
    "select id, url from attribute where id = any($1)",

    // select_quad
    "select id, quad_id, subject_id, predicate_id, attribute_id, object, type_id, language from quad where quad_id = $1",

    // insert_quads
    "insert into quad (quad_id, subject_id, predicate_id, attribute_id, object, type_id, language) select unnest($1::int[]), unnest($2::int[]), unnest($3::int[]), unnest($4::int[]), unnest($5::int[]), unnest($6::int[]), unnest($7::int[])",

    // delete_quads
    "delete from quad where quad_id = $1",
];

impl Statements {
    pub fn make(mut conn: Client) -> impl Future<Item = (Statements, Client), Error = Error> {
        let mut futures: Vec<_> = STATEMENTS.iter().map(|f| conn.prepare(f)).collect();
        stream::iter_ok(futures)
            .fold(vec![], |mut statements, item| item.map(|f| {
                statements.push(f);
                statements
            }))
            .map(move |mut stmts| {
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
