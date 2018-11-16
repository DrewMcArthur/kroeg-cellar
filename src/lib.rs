extern crate futures;
extern crate futures_state_stream;
extern crate jsonld;
extern crate kroeg_tap;
extern crate serde_json;
extern crate tokio_postgres;

use futures::{future, stream, Future, Stream};
use futures_state_stream::{StateStream, StreamEvent};
use jsonld::rdf::StringQuad;
use std::collections::HashSet;
use tokio_postgres::{error::Error, Client, Connection, Row, Statement};

mod statements;
pub use statements::*;

mod cache;
pub use cache::*;

struct CellarEntityStore {
    connection: Client,
    statements: Statements,
    cache: EntityCache,
}

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

impl From<&Row> for DatabaseQuad {
    fn from(row: &Row) -> DatabaseQuad {
        let contents = match (row.get(4), row.get(5), row.get(6), row.get(7)) {
            (Some(id), _, _, _) => DatabaseQuadContents::Id(id),
            (_, Some(contents), _, Some(language)) => DatabaseQuadContents::LanguageString {
                contents: contents,
                language: language,
            },
            (_, Some(contents), Some(type_id), _) => DatabaseQuadContents::Object {
                contents: contents,
                type_id: type_id,
            },
            _ => panic!("invalid quad contents; impossible"),
        };

        DatabaseQuad {
            id: row.get(0),
            quad_id: row.get(1),
            subject_id: row.get(2),
            predicate_id: row.get(3),
            contents: contents,
        }
    }
}

impl CellarEntityStore {
    /// Helper method to split up the entity store into its parts.
    fn unwrap(self) -> (Client, Statements, EntityCache) {
        (self.connection, self.statements, self.cache)
    }

    /// Takes a StateStream of Rows and stores them into the cache.
    fn cache_attribute_rows<T: Stream<Item = Row, Error = Error>>(
        conn: Client,
        cache: EntityCache,
        statements: Statements,
        stream: T,
    ) -> impl Future<Item = CellarEntityStore, Error = (Error, CellarEntityStore)> {
        stream.collect().then(move |future| {
            let mut store = CellarEntityStore {
                connection: conn,
                statements: statements,
                cache: cache,
            };

            match future {
                Ok(rows) => {
                    for row in rows {
                        store.cache.cache_attribute_row(row);
                    }

                    future::ok(store)
                }

                Err(e) => future::err((e, store)),
            }
        })
    }

    /// Returns a list of quads built from a StateStream.
    fn translate_quad_stream<T: Stream<Item = Row, Error = Error>>(
        conn: Client,
        cache: EntityCache,
        statements: Statements,
        stream: T,
    ) -> impl Future<Item = (Vec<DatabaseQuad>, CellarEntityStore), Error = (Error, CellarEntityStore)>
    {
        stream.map(|f| (&f).into()).collect().then(move |future| {
            let store = CellarEntityStore {
                connection: conn,
                statements: statements,
                cache: cache,
            };

            match future {
                Ok(items) => future::ok((items, store)),
                Err(e) => future::err((e, store)),
            }
        })
    }

    /// Takes a slice of Strings, queries them into the database, then stores them into the cache
    pub fn cache_uris(
        self,
        uris: &[String],
    ) -> impl Future<Item = CellarEntityStore, Error = (Error, CellarEntityStore)> {
        // XXX todo: 0 item optimization
        let (mut connection, statements, cache) = self.unwrap();
        let uncached: Vec<_> = uris
            .iter()
            .filter(|&f| !cache.uri_to_id.contains_key(f))
            .collect();
        let query = connection.query(&statements.upsert_attributes, &[&uncached]);

        CellarEntityStore::cache_attribute_rows(connection, cache, statements, query)
    }

    /// Takes a slice of IDs, queries them from the database, and stores them into the cache.
    pub fn cache_ids(
        self,
        ids: &[i32],
    ) -> impl Future<Item = CellarEntityStore, Error = (Error, CellarEntityStore)> {
        let (mut connection, statements, cache) = self.unwrap();
        let uncached: Vec<_> = ids
            .iter()
            .filter(|f| !cache.id_to_uri.contains_key(f))
            .collect();
        let query = connection.query(&statements.select_attributes, &[&uncached]);

        CellarEntityStore::cache_attribute_rows(connection, cache, statements, query)
    }

    /// Reads all the quads stored for a specific quad ID.
    fn read_quad(
        self,
        id: i32,
    ) -> impl Future<Item = (Vec<DatabaseQuad>, CellarEntityStore), Error = (Error, CellarEntityStore)>
    {
        let (mut connection, statements, cache) = self.unwrap();
        let query = connection.query(&statements.select_quad, &[&id]);

        CellarEntityStore::translate_quad_stream(connection, cache, statements, query)
    }

    /// Collects all IDs used inside the passed quads. Can be used to cache all the IDs.
    fn collect_quad_ids(&self, quads: &[DatabaseQuad]) -> HashSet<i32> {
        let mut out = HashSet::new();

        for quad in quads {
            out.insert(quad.quad_id);
            out.insert(quad.subject_id);
            out.insert(quad.predicate_id);
            match quad.contents {
                DatabaseQuadContents::Id(id) => out.insert(id),
                DatabaseQuadContents::Object { type_id: id, .. } => out.insert(id),
                _ => false,
            };
        }

        out
    }

    /// Translates the incoming quads into quads usable with the jsonld crate.
    pub fn translate_quads(
        self,
        quads: Vec<DatabaseQuad>,
    ) -> impl Future<Item = (Vec<StringQuad>, CellarEntityStore), Error = (Error, CellarEntityStore)>
    {
        let items: Vec<_> = self.collect_quad_ids(&quads).into_iter().collect();

        self.cache_ids(&items).map(|store| {
            (
                quads
                    .into_iter()
                    .map(|f| store.cache.translate_quad(f))
                    .collect(),
                store,
            )
        })
    }
}
