use postgres_async::{FrontendReceiver, Statement};
use postgres_async::types::AnyError;

pub struct Statements<'a> {
    pub upsert_attributes: Statement<'a>,
    pub select_attributes: Statement<'a>,
    pub select_quad: Statement<'a>,
    pub insert_quads: Statement<'a>,
    pub delete_quads: Statement<'a>,
    pub insert_collection: Statement<'a>,
    pub delete_collection: Statement<'a>,
    pub select_collection: Statement<'a>,
    pub select_collection_reverse: Statement<'a>,
    pub select_collection_inverse: Statement<'a>,
    pub find_collection: Statement<'a>,
    pub queue_item_pop: Statement<'a>,
    pub queue_item_put: Statement<'a>,
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

    // select_collection_inverse
    "select id, collection_id, object_id from collection_item where object_id = $1",

    // find_collection
    "select id, collection_id, object_id from collection_item where collection_id = $1 and id = $2",

    // queue_item_pop
    "delete from queue_item where id = (select id from queue_item order by id limit 1) returning event, data",

    // queue_item_put
    "insert into queue_item (event, data) values ($1, $2)"
];

impl<'a> Statements<'a> {
    pub async fn make(frontend: &impl FrontendReceiver<'a>) -> Result<Statements<'a>, AnyError> {
        Ok(Statements {
            upsert_attributes: Statement::parse(frontend, STATEMENTS[0]).await?,
            select_attributes: Statement::parse(frontend, STATEMENTS[1]).await?,
            select_quad: Statement::parse(frontend, STATEMENTS[2]).await?,
            insert_quads: Statement::parse(frontend, STATEMENTS[3]).await?,
            delete_quads: Statement::parse(frontend, STATEMENTS[4]).await?,
            insert_collection: Statement::parse(frontend, STATEMENTS[5]).await?,
            delete_collection: Statement::parse(frontend, STATEMENTS[6]).await?,
            select_collection: Statement::parse(frontend, STATEMENTS[7]).await?,
            select_collection_reverse: Statement::parse(frontend, STATEMENTS[8]).await?,
            select_collection_inverse: Statement::parse(frontend, STATEMENTS[9]).await?,
            find_collection: Statement::parse(frontend, STATEMENTS[10]).await?,
            queue_item_pop: Statement::parse(frontend, STATEMENTS[11]).await?,
            queue_item_put: Statement::parse(frontend, STATEMENTS[12]).await?,
        })
    }
}
