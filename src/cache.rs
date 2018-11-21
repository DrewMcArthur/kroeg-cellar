use crate::DatabaseQuad;
use jsonld::rdf::StringQuad;
use kroeg_tap::StoreItem;
use std::collections::HashMap;
use tokio_postgres::Row;

#[derive(Debug)]
pub struct EntityCache {
    pub id_to_uri: HashMap<i32, String>,
    pub uri_to_id: HashMap<String, i32>,

    pub object: HashMap<String, StoreItem>,
}

impl EntityCache {
    pub fn cache_attribute_row(&mut self, row: Row) {
        let id: i32 = row.get(0);
        let uri: String = row.get(1);

        self.id_to_uri.insert(id, uri.to_owned());
        self.uri_to_id.insert(uri, id);
    }

    pub fn translate_quad(&self, quad: DatabaseQuad) -> StringQuad {
        unimplemented!();
    }
}
