use jsonld::rdf::StringQuad;
use postgres_async::types::{AnyError, Row};
use std::fmt;

use crate::cache::EntityCache;
use crate::dbquad::{collect_quad_ids, DatabaseQuad};
use crate::types::CollectionItem;
use crate::CellarConnection;

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

    pub async fn insert_quad(
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

        if !until {
            out.reverse();
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
