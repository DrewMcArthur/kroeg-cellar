#[macro_use]
extern crate diesel;
extern crate jsonld;

pub mod models;
pub mod schema;

use diesel::dsl::any;
use diesel::pg::upsert::excluded;
use diesel::pg::PgConnection;
use diesel::prelude::*;

/// Bad hack for now.
pub use diesel::result::Error;

use std::collections::{HashMap, HashSet};
use std::fmt;

use jsonld::rdf::{QuadContents, StringQuad};

/// A client that talks to a database to store triples keyed by quad.
pub struct QuadClient {
    connection: PgConnection,
    attribute_id: HashMap<String, i32>,
    attribute_url: HashMap<i32, String>,
}

impl fmt::Debug for QuadClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "QuadClient {{ [...] }}")
    }
}

impl QuadClient {
    /// Creates a new `QuadClient` from a `PgConnnection`. Multiple clients
    /// can exist safely on one DB without interfering.
    pub fn new(connection: PgConnection) -> QuadClient {
        QuadClient {
            connection: connection,
            attribute_id: HashMap::new(),
            attribute_url: HashMap::new(),
        }
    }

    /// Gets a handle to the underlying connection.
    pub fn connection(&self) -> &PgConnection {
        &self.connection
    }

    /// Takes a list of attributes and caches their contents into this client.
    fn read_attributes(&mut self, vals: &Vec<models::Attribute>) {
        for val in vals {
            self.attribute_id.insert(val.url.to_owned(), val.id);
            self.attribute_url.insert(val.id, val.url.to_owned());
        }
    }

    /// Takes a `Vec` of IRIs, and stores them into the DB where needed,
    /// caching them.
    fn store_attributes(&mut self, vals: &Vec<&str>) -> Result<(), diesel::result::Error> {
        use models::NewAttribute;
        use schema::attribute::dsl::*;

        let to_write: Vec<_> = vals
            .iter()
            .filter(|f| !self.attribute_id.contains_key(**f))
            .map(|f| NewAttribute { url: f })
            .collect();

        if to_write.len() == 0 {
            return Ok(());
        }

        let attribute_results = diesel::insert_into(attribute)
            .values(&to_write)
            .on_conflict(url)
            .do_update()
            .set(url.eq(excluded(url)))
            .load(&self.connection)?;

        self.read_attributes(&attribute_results);

        Ok(())
    }

    /// Takes a `Vec` of database IDs and caches them into the local client.
    fn get_attributes(&mut self, vals: &Vec<i32>) -> Result<(), diesel::result::Error> {
        use schema::attribute::dsl::*;

        let to_read: Vec<_> = vals
            .iter()
            .filter(|f| !self.attribute_url.contains_key(f))
            .collect();
        if to_read.len() == 0 {
            return Ok(());
        }

        let attribute_results = attribute
            .filter(id.eq(any(to_read)))
            .load(&self.connection)?;

        self.read_attributes(&attribute_results);

        Ok(())
    }

    /// Gets a single attribute IRI from a database ID.
    pub fn get_attribute_url(&mut self, value: i32) -> Result<String, diesel::result::Error> {
        self.get_attributes(&vec![value])?;

        Ok(self.attribute_url[&value].to_owned())
    }

    /// Gets a single database ID from an attribute IRI.
    pub fn get_attribute_id(&mut self, value: &str) -> Result<i32, diesel::result::Error> {
        self.store_attributes(&vec![value])?;

        Ok(self.attribute_id[value].to_owned())
    }

    /// Takes a `Vec<Quad>` and ensures that all the database IDs that are used
    /// will be cached.
    fn preload_quads(&mut self, quads: &Vec<models::Quad>) -> Result<(), diesel::result::Error> {
        let mut required_ids = HashSet::new();

        for quad in quads {
            required_ids.insert(quad.subject_id);
            required_ids.insert(quad.predicate_id);

            if let Some(qval) = quad.attribute_id {
                required_ids.insert(qval);
            }

            if let Some(qval) = quad.type_id {
                required_ids.insert(qval);
            }
        }

        self.get_attributes(&(required_ids.into_iter().collect()))
    }

    /// Translates a single DB Quad into a `StringQuad`
    fn read_quad(&self, quad: &models::Quad) -> StringQuad {
        let contents = if let Some(attribute_id) = quad.attribute_id {
            QuadContents::Id(self.attribute_url[&attribute_id].to_owned())
        } else if let Some(type_id) = quad.type_id {
            QuadContents::Object(
                self.attribute_url[&type_id].to_owned(),
                quad.object.as_ref().unwrap().to_owned(),
                quad.language.as_ref().map(|f| f.to_owned()),
            )
        } else {
            unreachable!();
        };

        StringQuad {
            subject_id: self.attribute_url[&quad.subject_id].to_owned(),
            predicate_id: self.attribute_url[&quad.predicate_id].to_owned(),
            contents: contents,
        }
    }

    /// Reads a list of triples from the database, using a graph ID as key.
    pub fn read_quads(&mut self, quadid: &str) -> Result<Vec<StringQuad>, diesel::result::Error> {
        let quadid = self.get_attribute_id(quadid)?;

        use schema::quad::dsl::*;

        let quads: Vec<models::Quad> = quad.filter(quad_id.eq(quadid)).load(&self.connection)?;

        self.preload_quads(&quads)?;

        Ok(quads.into_iter().map(|f| self.read_quad(&f)).collect())
    }

    fn prestore_quads(&mut self, quads: &Vec<StringQuad>) -> Result<(), diesel::result::Error> {
        let mut required_ids: HashSet<&str> = HashSet::new();

        for quad in quads {
            required_ids.insert(&quad.subject_id);
            required_ids.insert(&quad.predicate_id);
            match quad.contents {
                QuadContents::Id(ref data) => required_ids.insert(&*data),
                QuadContents::Object(ref data, _, _) => required_ids.insert(&*data),
            };
        }

        self.store_attributes(&(required_ids.into_iter().collect()))
    }

    fn write_quad(&self, quad_id: i32, quad: StringQuad) -> models::InsertableQuad {
        let (attribute_id, type_id, object, lang) = match quad.contents {
            QuadContents::Id(data) => (Some(self.attribute_id[&data]), None, None, None),
            QuadContents::Object(data, object, lang) => {
                (None, Some(self.attribute_id[&data]), Some(object), lang)
            }
        };

        models::InsertableQuad {
            quad_id: quad_id,
            subject_id: self.attribute_id[&quad.subject_id],
            predicate_id: self.attribute_id[&quad.predicate_id],
            attribute_id: attribute_id,
            type_id: type_id,
            object: object,
            language: lang,
        }
    }

    /// Store a list of quads in the DB, keyed by graph ID.
    pub fn write_quads(
        &mut self,
        quadid: &str,
        items: Vec<StringQuad>,
    ) -> Result<(), diesel::result::Error> {
        self.prestore_quads(&items)?;

        let quadid = self.get_attribute_id(quadid)?;

        use schema::quad::dsl::*;

        let items: Vec<_> = items
            .into_iter()
            .map(|f| self.write_quad(quadid, f))
            .collect();

        diesel::delete(quad.filter(quad_id.eq(quadid))).execute(&self.connection)?;
        diesel::insert_into(quad)
            .values(&items)
            .execute(&self.connection)?;

        Ok(())
    }
}
