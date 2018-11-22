use crate::{DatabaseQuad, DatabaseQuadContents};
use jsonld::rdf::{StringQuad, QuadContents};
use kroeg_tap::StoreItem;
use std::collections::HashMap;
use tokio_postgres::Row;

#[derive(Debug)]
pub struct EntityCache {
    pub id_to_uri: HashMap<i32, String>,
    pub uri_to_id: HashMap<String, i32>,

    pub object: HashMap<String, StoreItem>,
}

/*

pub enum DatabaseQuadContents {
    Id(i32),
    Object { contents: String, type_id: i32 },
    LanguageString { contents: String, language: String },
}

pub struct DatabaseQuad {
    pub id: i32,
    pub quad_id: i32,
    pub subject_id: i32,
    pub predicate_id: i32,
    pub contents: DatabaseQuadContents,
}


#[derive(Debug, Clone)]
/// The contents of a single quad, which is either an ID reference or an Object.
pub enum QuadContents {
    /// An ID
    Id(String),

    /// An object, which consists of respectively a type, content, and optionally a language.
    Object(String, String, Option<String>),
}

#[derive(Debug, Clone)]
/// A single quad, consisting of a subject, predicate, and contents.
pub struct StringQuad {
    pub subject_id: String,
    pub predicate_id: String,

    pub contents: QuadContents,
}
*/

impl EntityCache {
    pub fn cache_attribute_row(&mut self, row: Row) {
        let id: i32 = row.get(0);
        let uri: String = row.get(1);

        self.id_to_uri.insert(id, uri.to_owned());
        self.uri_to_id.insert(uri, id);
    }

    pub fn translate_quad(&self, quad: DatabaseQuad) -> StringQuad {
        let contents = match quad.contents {
            DatabaseQuadContents::Id(id) => QuadContents::Id(self.id_to_uri[&id].clone()),
            DatabaseQuadContents::Object { contents, type_id } => QuadContents::Object(self.id_to_uri[&type_id].clone(), contents, None),
            DatabaseQuadContents::LanguageString { contents, language } => QuadContents::Object("http://www.w3.org/2000/01/rdf-schema#langString".to_owned(), contents, Some(language)),
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
