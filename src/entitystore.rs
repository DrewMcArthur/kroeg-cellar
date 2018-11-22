use kroeg_tap::{EntityStore, StoreItem, CollectionPointer, QuadQuery};
use tokio_postgres::{error::Error};
use futures::{future, stream, Future, Stream};
use crate::CellarEntityStore;
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld};
use serde_json::Value as JValue;
use std::collections::HashMap;

fn get_ids(quad: &StringQuad, set: &mut HashSet) {
    match quad.contents {
        QuadContents::Id(id) => set.insert(id.to_owned()),
        QuadContents::Object(type_id, _, _) => set.insert(type_id.to_owned()),
    };

    set.insert(quad.subject_id.to_owned());
    set.insert(quad.predicate_id.to_owned());
}

/// An entity store, storing JSON-LD `Entity` objects.
impl EntityStore for CellarEntityStore {
    /// The error type that will be returned if this store fails to get or put
    /// the `StoreItem`
    type Error = Error;

    // ---

    /// The `Future` that is returned when `get`ting a `StoreItem`.
    type GetFuture = Box<Future<Item = (Option<StoreItem>, Self), Error = (Error, Self)>
        
        + Send>;

    /// Gets a single `StoreItem` from the store. Missing entities are no error,
    /// but instead returns a `None`.
    fn get(self, path: String, _local: bool) -> Self::GetFuture {
        let id = path.to_owned();

        Box::new(self.cache_uris(&[path.to_owned()])
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
            }))
    }

    // ---

    /// The `Future` that is returned when `put`ting a `StoreItem`.
    type StoreFuture = Box<Future<Item = (StoreItem, Self), Error = (Error, Self)>  + Send>;

    /// Stores a single `StoreItem` into the store.
    ///
    /// To delete an Entity, set its type to as:Tombstone. This may
    /// instantly remove it, or queue it for possible future deletion.
    fn put(self, path: String, item: StoreItem) -> Self::StoreFuture {
        let item = item.to_json();

        let rdf = jsonld_to_rdf(item, &mut StoreItemNodeGenerator::new()).unwrap();
        let mut set = HashSet::new();
        let quads = rdf.get("@default").unwrap().clone();
        for quad in &quads {
            get_ids(quad, &mut set);
        }

        

        Box::new(future::ok((item, self)))
    }

    // -----

    /// The `Future` that is returned when querying the database.
    type QueryFuture = Box<Future<Item = (Vec<Vec<String>>, Self), Error = (Error, Self)>
        
        + Send>;

    /// Queries the entire store for a specific set of parameters.
    /// The return value is a list for every result in the database that matches the query.
    /// The array elements are in numeric order of the placeholders.
    fn query(self, query: Vec<QuadQuery>) -> Self::QueryFuture {
        Box::new(future::ok((vec![], self)))
    }

    // -----

    /// The `Future` that is returned when reading the collection data.
    type ReadCollectionFuture = Box<Future<Item = (CollectionPointer, Self), Error = (Error, Self)>
        
        + Send>;

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
        
        Box::new(future::ok((ptr, self)))
    }

    // -----

    type FindCollectionFuture = Box<Future<Item = (CollectionPointer, Self), Error = (Error, Self)>  + Send>;

    /// Finds an item in a collection. The result will contain cursors to just before and after the item, if it exists.
    fn find_collection(self, path: String, item: String) -> Self::FindCollectionFuture {
        let ptr = CollectionPointer {
            items: vec![],
            after: None,
            before: None,
            count: None,
        };
        
        Box::new(future::ok((ptr, self)))
    }

    // -----

    /// The `Future` that is returned when writing into a collection.
    type WriteCollectionFuture = Box<Future<Item = Self, Error = (Error, Self)>  + Send>;

    /// Inserts an item into the back of the collection.
    fn insert_collection(self, path: String, item: String) -> Self::WriteCollectionFuture {
        Box::new(future::ok(self))
    }

    // -----

    type ReadCollectionInverseFuture = Box<Future<Item = (CollectionPointer, Self), Error = (Error, Self)>  + Send>;

    /// Finds all the collections containing a specific object.
    fn read_collection_inverse(self, item: String) -> Self::ReadCollectionInverseFuture {
        let ptr = CollectionPointer {
            items: vec![],
            after: None,
            before: None,
            count: None,
        };
        
        Box::new(future::ok((ptr, self)))
    }

    // -----

    type RemoveCollectionFuture = Box<Future<Item = Self, Error = (Error, Self)>  + Send>;

    /// Removes an item from the collection.
    fn remove_collection(self, path: String, item: String) -> Self::RemoveCollectionFuture {
        Box::new(future::ok(self))
    }
}