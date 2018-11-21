use kroeg_tap::{EntityStore, StoreItem, CollectionPointer, QuadQuery};
use tokio_postgres::{error::Error};
use futures::{future, stream, Future, Stream};
use crate::CellarEntityStore;
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld};
use serde_json::Value as JValue;
use std::collections::HashMap;

/// An entity store, storing JSON-LD `Entity` objects.
impl EntityStore for CellarEntityStore {
    /// The error type that will be returned if this store fails to get or put
    /// the `StoreItem`
    type Error = Error;

    // ---

    /// The `Future` that is returned when `get`ting a `StoreItem`.
    existential type GetFuture: Future<Item = (Option<StoreItem>, Self), Error = (Error, Self)>
        + 'static
        + Send;

    /// Gets a single `StoreItem` from the store. Missing entities are no error,
    /// but instead returns a `None`.
    fn get(self, path: String, _local: bool) -> Self::GetFuture {
        let id = path.to_owned();

        self.cache_uris(&[path.to_owned()])
            .and_then(move |store| {
                let i = store.cache.uri_to_id[&path];
                store.read_quad(i)
            })
            .and_then(move |(items, store)| store.translate_quads(items))
            .map(move |(quads, store)| {
                if quads.len() == 0 {
                    return (None, store);
                }

                let mut hash = HashMap::new();
                hash.insert("@default".to_owned(), quads);
                match rdf_to_jsonld(hash, true, false) {
                    JValue::Object(jval) => {
                        let jval = JValue::Array(jval.into_iter().map(|(_, b)| b).collect());
                        (StoreItem::parse(&id, jval).ok(), store)
                    }

                    _ => unreachable!(),
                }
            })
    }

    // ---

    /// The `Future` that is returned when `put`ting a `StoreItem`.
    existential type StoreFuture: Future<Item = (StoreItem, Self), Error = (Error, Self)> + 'static + Send;

    /// Stores a single `StoreItem` into the store.
    ///
    /// To delete an Entity, set its type to as:Tombstone. This may
    /// instantly remove it, or queue it for possible future deletion.
    fn put(self, path: String, item: StoreItem) -> Self::StoreFuture {
        future::ok((item, self))
    }

    // -----

    /// The `Future` that is returned when querying the database.
    existential type QueryFuture: Future<Item = (Vec<Vec<String>>, Self), Error = (Error, Self)>
        + 'static
        + Send;

    /// Queries the entire store for a specific set of parameters.
    /// The return value is a list for every result in the database that matches the query.
    /// The array elements are in numeric order of the placeholders.
    fn query(self, query: Vec<QuadQuery>) -> Self::QueryFuture {
        future::ok((vec![], self))
    }

    // -----

    /// The `Future` that is returned when reading the collection data.
    existential type ReadCollectionFuture: Future<Item = (CollectionPointer, Self), Error = (Error, Self)>
        + 'static
        + Send;

    /// Reads N amount of items from the collection corresponding to a specific ID. If a cursor is passed,
    /// it can be used to paginate.
    fn read_collection(
        self,
        path: String,
        count: Option<u32>,
        cursor: Option<String>,
    ) -> Self::ReadCollectionFuture {
        let ptr = CollectionPointer {
            items: vec![],
            after: None,
            before: None,
            count: None,
        };
        
        future::ok((ptr, self))
    }

    // -----

    existential type FindCollectionFuture: Future<Item = (CollectionPointer, Self), Error = (Error, Self)> + 'static + Send;

    /// Finds an item in a collection. The result will contain cursors to just before and after the item, if it exists.
    fn find_collection(self, path: String, item: String) -> Self::FindCollectionFuture {
        let ptr = CollectionPointer {
            items: vec![],
            after: None,
            before: None,
            count: None,
        };
        
        future::ok((ptr, self))
    }

    // -----

    /// The `Future` that is returned when writing into a collection.
    existential type WriteCollectionFuture: Future<Item = Self, Error = (Error, Self)> + 'static + Send;

    /// Inserts an item into the back of the collection.
    fn insert_collection(self, path: String, item: String) -> Self::WriteCollectionFuture {
        future::ok(self)
    }

    // -----

    existential type ReadCollectionInverseFuture: Future<Item = (CollectionPointer, Self), Error = (Error, Self)> + 'static + Send;

    /// Finds all the collections containing a specific object.
    fn read_collection_inverse(self, item: String) -> Self::ReadCollectionInverseFuture {
        let ptr = CollectionPointer {
            items: vec![],
            after: None,
            before: None,
            count: None,
        };
        
        future::ok((ptr, self))
    }

    // -----

    existential type RemoveCollectionFuture: Future<Item = Self, Error = (Error, Self)> + 'static + Send;

    /// Removes an item from the collection.
    fn remove_collection(self, path: String, item: String) -> Self::RemoveCollectionFuture {
        future::ok(self)
    }
}