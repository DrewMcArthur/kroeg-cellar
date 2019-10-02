use async_std::net::TcpStream;
use jsonld::rdf::StringQuad;
use postgres_async::{AnyError, Connection, Row};
use std::collections::HashSet;
use std::fmt;

mod statements;
pub use statements::*;

mod cache;
pub use cache::*;

mod entitystore;
mod queuestore;

/// A connection to a Kroeg PostgreSQL-backed database.
pub struct CellarConnection {
    connection: Connection<'static>,
    statements: Statements<'static>,
}

/// A wrapper for a CellarConnection that implements the EntityStore and QueueStore traits.
/// Multiple `CellarEntityStore`s may exist for one single `CellarConnection`, but they cannot
///  create a transaction or span more than one transaction.
pub struct CellarEntityStore<'a> {
    connection: &'a CellarConnection,
    pub cache: EntityCache,
}

impl<'a> fmt::Debug for CellarEntityStore<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CellarEntityStore")
            .field("cache", &self.cache)
            .finish()
    }
}

/// The contents of a single database quad.
pub enum DatabaseQuadContents {
    /// This quad points to another subject.
    Id(i32),

    /// This quad consists of a type id, and the contents (serialized as string, like RDF)
    Object { contents: String, type_id: i32 },

    /// This quad consists of a string, and an associated language.
    LanguageString { contents: String, language: String },
}

/// The raw contents of a single database quad.
pub struct DatabaseQuad {
    pub id: i32,
    pub quad_id: i32,
    pub subject_id: i32,
    pub predicate_id: i32,
    pub contents: DatabaseQuadContents,
}

impl DatabaseQuad {
    fn make_from_row(row: &Row) -> DatabaseQuad {
        let contents = match (
            row.get(4).unwrap(),
            row.get(5).unwrap(),
            row.get(6).unwrap(),
            row.get(7).unwrap(),
        ) {
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
            id: row.get(0).unwrap().unwrap(),
            quad_id: row.get(1).unwrap().unwrap(),
            subject_id: row.get(2).unwrap().unwrap(),
            predicate_id: row.get(3).unwrap().unwrap(),
            contents: contents,
        }
    }
}

pub struct CollectionItem {
    pub id: i32,
    pub collection_id: i32,
    pub object_id: i32,
}

impl CollectionItem {
    fn make_from_row(row: &Row) -> CollectionItem {
        CollectionItem {
            id: row.get(0).unwrap().unwrap(),
            collection_id: row.get(1).unwrap().unwrap(),
            object_id: row.get(2).unwrap().unwrap(),
        }
    }
}

/// Collects all IDs used inside the passed quads. Can be used to cache all the IDs.
fn collect_quad_ids(quads: &[DatabaseQuad]) -> HashSet<i32> {
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

impl CellarConnection {
    /// Connects to a given postgres database over TCP, with no TLS.
    pub async fn connect(
        address: &str,
        username: &str,
        pass: &str,
        db: &str,
    ) -> Result<CellarConnection, AnyError> {
        let stream = TcpStream::connect(address).await?;

        let connection =
            postgres_async::connect(stream, username.to_owned(), pass.to_owned(), db.to_owned())
                .await?;
        let statements = Statements::make(&connection).await?;

        Ok(CellarConnection {
            connection,
            statements,
        })
    }
}

impl<'a> CellarEntityStore<'a> {
    pub fn new(connection: &'a CellarConnection) -> CellarEntityStore<'a> {
        CellarEntityStore {
            connection,
            cache: EntityCache::new(),
        }
    }

    /// Translates the incoming quads into quads usable with the jsonld crate.
    pub async fn translate_quads(
        &mut self,
        quads: Vec<DatabaseQuad>,
    ) -> Result<Vec<StringQuad>, AnyError> {
        let items: Vec<_> = collect_quad_ids(&quads).into_iter().collect();

        self.cache_ids(&items).await?;

        Ok(quads
            .into_iter()
            .map(|f| self.cache.translate_quad(f))
            .collect())
    }

    /// Takes a slice of Strings, queries them into the database, then stores them into the cache
    pub async fn cache_uris(&mut self, uris: &[String]) -> Result<(), AnyError> {
        let uncached: Vec<_> = uris
            .iter()
            .filter(|&f| !self.cache.uri_to_id.contains_key(f))
            .collect();

        if uncached.is_empty() {
            return Ok(());
        }

        let mut bound = self
            .connection
            .statements
            .upsert_attributes
            .bind(&self.connection.connection, &[&uncached])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            let item = item?;

            self.cache.cache_attribute_row(item);
        }

        Ok(())
    }

    /// Takes a slice of IDs, queries them from the database, and stores them into the cache.
    pub async fn cache_ids(&mut self, ids: &[i32]) -> Result<(), AnyError> {
        let uncached: Vec<_> = ids
            .iter()
            .filter(|f| !self.cache.id_to_uri.contains_key(f))
            .collect();

        if uncached.is_empty() {
            return Ok(());
        }

        let mut bound = self
            .connection
            .statements
            .select_attributes
            .bind(&self.connection.connection, &[&uncached])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            let item = item?;

            self.cache.cache_attribute_row(item);
        }

        Ok(())
    }

    /// Reads all the quads stored for a specific quad ID.
    pub async fn read_quad(&mut self, id: i32) -> Result<Vec<DatabaseQuad>, AnyError> {
        let mut bound = self
            .connection
            .statements
            .select_quad
            .bind(&self.connection.connection, &[&id])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        let mut out = Vec::new();
        while let Some(item) = query.next().await {
            out.push(DatabaseQuad::make_from_row(&item?));
        }

        Ok(out)
    }

    /// Removes all the quads stored for a specific quad ID.
    pub async fn delete_quad(&mut self, id: i32) -> Result<(), AnyError> {
        let mut bound = self
            .connection
            .statements
            .delete_quads
            .bind(&self.connection.connection, &[&id])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            item?;
        }

        Ok(())
    }

    async fn insert_quad(
        &mut self,
        data: &[&dyn postgres_async::types::Serializable],
    ) -> Result<(), AnyError> {
        let mut bound = self
            .connection
            .statements
            .insert_quads
            .bind(&self.connection.connection, data)
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            item?;
        }

        Ok(())
    }

    pub async fn insert_collection(
        &mut self,
        collection: i32,
        object: i32,
    ) -> Result<(), AnyError> {
        let mut bound = self
            .connection
            .statements
            .insert_collection
            .bind(&self.connection.connection, &[&collection, &object])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            item?;
        }

        Ok(())
    }

    pub async fn delete_collection(
        &mut self,
        collection: i32,
        object: i32,
    ) -> Result<(), AnyError> {
        let mut bound = self
            .connection
            .statements
            .delete_collection
            .bind(&self.connection.connection, &[&collection, &object])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            item?;
        }

        Ok(())
    }

    pub async fn select_collection(
        &mut self,
        collection: i32,
        offset: i32,
        limit: i32,
        until: bool,
    ) -> Result<Vec<CollectionItem>, AnyError> {
        let mut out = Vec::new();

        let mut bound = if until {
            &self.connection.statements.select_collection_reverse
        } else {
            &self.connection.statements.select_collection
        }
        .bind(
            &self.connection.connection,
            &[&collection, &offset, &(limit as i64)],
        )
        .await?;
        let mut query = bound.execute(&self.connection.connection).await?;
        while let Some(item) = query.next().await {
            let item = item?;

            out.push(CollectionItem::make_from_row(&item));
        }

        Ok(out)
    }

    pub async fn select_collection_inverse(
        &mut self,
        object: i32,
    ) -> Result<Vec<CollectionItem>, AnyError> {
        let mut out = Vec::new();

        let mut bound = self
            .connection
            .statements
            .select_collection_inverse
            .bind(&self.connection.connection, &[&object])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;
        while let Some(item) = query.next().await {
            let item = item?;

            out.push(CollectionItem {
                id: item.get(0)?.unwrap(),
                collection_id: item.get(1)?.unwrap(),
                object_id: item.get(2)?.unwrap(),
            });
        }

        Ok(out)
    }

    pub async fn collection_contains(
        &mut self,
        collection: i32,
        item: i32,
    ) -> Result<bool, AnyError> {
        let mut bound = self
            .connection
            .statements
            .find_collection
            .bind(&self.connection.connection, &[&collection, &item])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        let mut contains = false;
        while let Some(item) = query.next().await {
            item?;

            contains = true;
        }

        Ok(contains)
    }

    pub async fn do_query(
        &mut self,
        q: String,
        data: &[&dyn postgres_async::types::Serializable],
    ) -> Result<Vec<Row>, AnyError> {
        let statement = postgres_async::Statement::parse(&self.connection.connection, &q).await?;
        let mut bound = statement.bind(&self.connection.connection, data).await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        let mut out = Vec::new();
        while let Some(item) = query.next().await {
            out.push(item?);
        }

        Ok(out)
    }

    pub async fn pop_queue(&mut self) -> Result<Option<(String, String)>, AnyError> {
        let mut bound = self
            .connection
            .statements
            .queue_item_pop
            .bind(&self.connection.connection, &[])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;
        let mut output = None;

        while let Some(item) = query.next().await {
            let item = item?;

            if output.is_none() {
                output = Some((item.get(0)?.unwrap(), item.get(1)?.unwrap()));
            }
        }

        Ok(output)
    }

    pub async fn push_queue(&mut self, event: String, data: String) -> Result<(), AnyError> {
        let mut bound = self
            .connection
            .statements
            .queue_item_put
            .bind(&self.connection.connection, &[&event, &data])
            .await?;
        let mut query = bound.execute(&self.connection.connection).await?;

        while let Some(item) = query.next().await {
            item?;
        }

        Ok(())
    }
}
