use futures::{future, stream, Future, Stream};
use tokio_postgres::{error::Error, Client, Statement};

pub struct Statements {
    pub upsert_attributes: Statement,
    pub select_attributes: Statement,
    pub select_quad: Statement,
    pub insert_quads: Statement,
    pub delete_quads: Statement,
    pub insert_collection: Statement,
    pub delete_collection: Statement,
    pub select_collection: Statement,
    pub select_collection_reverse: Statement,
    pub find_collection: Statement,
    pub queue_item_pop: Statement,
    pub queue_item_put: Statement,
}

const STATEMENTS: &[&'static str] = &[
    // upsert_attributes
    "with new_rows as (insert into attribute (url) select unnest($1::text[]) on conflict (url) do nothing returning id, url) select id, url from new_rows union distinct select id, url from attribute where url = any($1::text[])",

    // select_attributes
    "select id, url from attribute where id = any($1)",

    // select_quad
    "select id, quad_id, subject_id, predicate_id, attribute_id, object, type_id, language from quad where quad_id = $1",

    // insert_quads
    "insert into quad (quad_id, subject_id, predicate_id, attribute_id, object, type_id, language) select unnest($1::int[]), unnest($2::int[]), unnest($3::int[]), unnest($4::int[]), unnest($5::text[]), unnest($6::int[]), unnest($7::text[])",

    // delete_quads
    "delete from quad where quad_id = $1",

    // insert_collection
    "insert into collection_item (collection_id, object_id) values ($1, $2) on conflict do nothing",

    // delete_collection
    "delete from collection_item where collection_id = $1 and object_id = $2",

    // select_collection
    "select id, collection_id, object_id from collection_item where collection_id = $1 and id >= $2 order by id asc limit $3",

    // select_collection_reverse
    "select id, collection_id, object_id from collection_item where collection_id = $1 and id <= $2 order by id desc limit $3",

    // find_collection
    "select id, collection_id, object_id from collection_item where collection_id = $1 and id = $2",

    // queue_item_pop
    "delete from queue_item where id = (select id from queue_item order by id limit 1) returning event, data",

    // queue_item_put
    "insert into queue_item (event, data) values ($1, $2)"
];

impl Statements {
    pub fn make(mut conn: Client) -> impl Future<Item = (Statements, Client), Error = Error> {
        let mut futures: Vec<_> = STATEMENTS.iter().map(|f| conn.prepare(f)).collect();
        stream::iter_ok(futures)
            .fold(vec![], |mut statements, item| {
                item.map(|f| {
                    statements.push(f);
                    statements
                })
            })
            .map(move |mut stmts| {
                (
                    Statements {
                        upsert_attributes: stmts.remove(0),
                        select_attributes: stmts.remove(0),
                        select_quad: stmts.remove(0),
                        insert_quads: stmts.remove(0),
                        delete_quads: stmts.remove(0),
                        insert_collection: stmts.remove(0),
                        delete_collection: stmts.remove(0),
                        select_collection: stmts.remove(0),
                        select_collection_reverse: stmts.remove(0),
                        find_collection: stmts.remove(0),
                        queue_item_pop: stmts.remove(0),
                        queue_item_put: stmts.remove(0),
                    },
                    conn,
                )
            })
    }
}
