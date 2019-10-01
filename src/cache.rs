use crate::{DatabaseQuad, DatabaseQuadContents};
use jsonld::rdf::{QuadContents, StringQuad};
use kroeg_tap::StoreItem;
use postgres_async::Row;
use std::collections::HashMap;

#[derive(Debug)]
pub struct EntityCache {
    pub id_to_uri: HashMap<i32, String>,
    pub uri_to_id: HashMap<String, i32>,

    pub object: HashMap<String, StoreItem>,
}

impl EntityCache {
    pub fn cache_attribute_row(&mut self, row: Row) {
        let id: i32 = row.get(0).unwrap().unwrap();
        let uri: String = row.get(1).unwrap().unwrap();

        self.id_to_uri.insert(id, uri.to_owned());
        self.uri_to_id.insert(uri, id);
    }

    pub fn translate_quad(&self, quad: DatabaseQuad) -> StringQuad {
        let contents = match quad.contents {
            DatabaseQuadContents::Id(id) => QuadContents::Id(self.id_to_uri[&id].clone()),
            DatabaseQuadContents::Object { contents, type_id } => {
                QuadContents::Object(self.id_to_uri[&type_id].clone(), contents, None)
            }
            DatabaseQuadContents::LanguageString { contents, language } => QuadContents::Object(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_owned(),
                contents,
                Some(language),
            ),
        };

        StringQuad {
            subject_id: self.id_to_uri[&quad.subject_id].clone(),
            predicate_id: self.id_to_uri[&quad.predicate_id].clone(),
            contents: contents,
        }
    }

    pub(crate) fn new() -> Self {
        EntityCache {
            id_to_uri: HashMap::new(),
            uri_to_id: HashMap::new(),
            object: HashMap::new(),
        }
    }
}
