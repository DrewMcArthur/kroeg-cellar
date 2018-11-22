extern crate futures;
extern crate jsonld;
extern crate kroeg_tap;
extern crate serde_json;
extern crate tokio_postgres;

use futures::{future::{self, Either}, stream, Future, Stream, Poll, Async};
use jsonld::rdf::StringQuad;
use std::collections::HashSet;
use tokio_postgres::{error::Error, Client, Connection, Row, Statement, TlsMode};
use std::fmt;
use std::time::{Instant, Duration};

mod statements;
pub use statements::*;

mod cache;
pub use cache::*;

mod entitystore;

pub struct CellarEntityStore {
    pub(crate) client: Client,
    pub(crate) connection: Connection,
    pub(crate) statements: Statements,
    pub(crate) cache: EntityCache,
}

impl fmt::Debug for CellarEntityStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CellarEntityStore")
            .field("connection", &format_args!("[redacted]"))
            .field("statements", &format_args!("[there are statements.]"))
            .field("cache", &self.cache)
            .finish()
    }
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

struct ButAlsoPoll<T: Future>(Option<Connection>, T, Instant);
impl <T: Future> ButAlsoPoll<T> {
    pub fn new(connection: Connection, future: T) -> Self {
        ButAlsoPoll(Some(connection), future, Instant::now())
    }
}

impl <T: Future<Error = Error>> Future for ButAlsoPoll<T> {
    type Item = (Connection, T::Item);
    type Error = (Connection, Error);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Err(e) = self.0.as_mut().unwrap().poll() {
            return Err((self.0.take().unwrap(), e));
        }

        match self.1.poll() {
            Ok(Async::Ready(ready)) => {
                let duration = Instant::now() - self.2;
                println!("took {:?}", duration);
                Ok(Async::Ready((self.0.take().unwrap(), ready)))
            }

            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err((self.0.take().unwrap(), e)),
        }
    }
}

impl CellarEntityStore {
    pub fn new(params: &str) -> impl Future<Item = CellarEntityStore, Error = Error> {
        let params = params.parse().unwrap();
        let start = Instant::now();
        tokio_postgres::connect(params, TlsMode::None)
            .and_then(move |(client, connection)| {
                ButAlsoPoll::new(connection, Statements::make(client))
                    .map_err(|(conn, e)| e)
            })
            .map(move |(conn, (stmts, client))| {
                let duration = Instant::now() - start;
                CellarEntityStore {
                    client: client,
                    connection: conn,
                    statements: stmts,
                    cache: EntityCache::new()
                }
            })
    }

    /// Helper method to split up the entity store into its parts.
    fn unwrap(self) -> (Client, Connection, Statements, EntityCache) {
        (self.client, self.connection, self.statements, self.cache)
    }

    /// Takes a StateStream of Rows and stores them into the cache.
    fn cache_attribute_rows<T: Stream<Item = Row, Error = Error>>(
        client: Client,
        connection: Connection,
        cache: EntityCache,
        statements: Statements,
        stream: T,
    ) -> impl Future<Item = CellarEntityStore, Error = (Error, CellarEntityStore)> {
        ButAlsoPoll::new(connection, stream.collect()).then(move |future| {
            match future {
                Ok((connection, rows)) => {
                    let mut store = CellarEntityStore {
                        client: client,
                        connection: connection,
                        statements: statements,
                        cache: cache,
                    };

                    for row in rows {
                        store.cache.cache_attribute_row(row);
                    }

                    future::ok(store)
                }

                Err((connection, e)) => {
                    let mut store = CellarEntityStore {
                        client: client,
                        connection: connection,
                        statements: statements,
                        cache: cache,
                    };

                    future::err((e, store))
                }
            }
        })
    }

    /// Returns a list of quads built from a StateStream.
    fn translate_quad_stream<T: Stream<Item = Row, Error = Error>>(
        client: Client,
        connection: Connection,
        cache: EntityCache,
        statements: Statements,
        stream: T,
    ) -> impl Future<Item = (Vec<DatabaseQuad>, CellarEntityStore), Error = (Error, CellarEntityStore)>
    {
        ButAlsoPoll::new(connection, stream.map(|f| (&f).into()).collect()).then(move |future| {
            match future {
                Ok((connection, items)) => {
                    let store = CellarEntityStore {
                        client: client,
                        connection: connection,
                        statements: statements,
                        cache: cache,
                    };

                    future::ok((items, store))
                }
                Err((connection, e)) => {
                    let store = CellarEntityStore {
                        client: client,
                        connection: connection,
                        statements: statements,
                        cache: cache,
                    };

                    future::err((e, store))
                }
            }
        })
    }

    /// Takes a slice of Strings, queries them into the database, then stores them into the cache
    pub fn cache_uris(
        self,
        uris: &[String],
    ) -> impl Future<Item = CellarEntityStore, Error = (Error, CellarEntityStore)> {
        let uncached: Vec<_> = uris
            .iter()
            .filter(|&f| !self.cache.uri_to_id.contains_key(f))
            .collect();

        if uncached.len() > 0 {
            let (mut client, connection, statements, cache) = self.unwrap();
            let query = client.query(&statements.upsert_attributes, &[&uncached]);

            Either::A(CellarEntityStore::cache_attribute_rows(client, connection, cache, statements, query))
        } else {
            Either::B(future::ok(self))
        }
    }

    /// Takes a slice of IDs, queries them from the database, and stores them into the cache.
    pub fn cache_ids(
        self,
        ids: &[i32],
    ) -> impl Future<Item = CellarEntityStore, Error = (Error, CellarEntityStore)> {
        let uncached: Vec<_> = ids
            .iter()
            .filter(|f| !self.cache.id_to_uri.contains_key(f))
            .collect();

        if uncached.len() > 0 {
            let (mut client, connection, statements, cache) = self.unwrap();
            let query = client.query(&statements.select_attributes, &[&uncached]);

            Either::A(CellarEntityStore::cache_attribute_rows(client, connection, cache, statements, query))
        } else {
            Either::B(future::ok(self))
        }
    }

    /// Reads all the quads stored for a specific quad ID.
    fn read_quad(
        self,
        id: i32,
    ) -> impl Future<Item = (Vec<DatabaseQuad>, CellarEntityStore), Error = (Error, CellarEntityStore)>
    {
        let (mut client, connection, statements, cache) = self.unwrap();
        let query = client.query(&statements.select_quad, &[&id]);

        CellarEntityStore::translate_quad_stream(client, connection, cache, statements, query)
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
