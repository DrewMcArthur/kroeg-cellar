use super::QuadClient;

use diesel::prelude::*;
use diesel::result::Error;
use diesel::{delete, insert_into};

use futures::future;
use futures::prelude::*;

use std::collections::HashMap;

use jsonld::nodemap::DefaultNodeGenerator;
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld};
use kroeg_tap::{CollectionPointer, EntityStore, StoreItem};

impl EntityStore for QuadClient {
    type Error = Error;
    type GetFuture = Box<Future<Item = Option<StoreItem>, Error = Self::Error> + Send>;
    type StoreFuture = Box<Future<Item = StoreItem, Error = Self::Error> + Send>;

    type ReadCollectionFuture = future::FutureResult<CollectionPointer, Self::Error>;
    type WriteCollectionFuture = future::FutureResult<(), Self::Error>;

    fn get(&self, path: String) -> Self::GetFuture {
        let cache = self.cache.borrow_mut();
        if cache.contains_key(&path) {
            Box::new(future::ok(cache[&path].clone()))
        } else {
            let quads = match self.read_quads(&path) {
                Ok(quads) => quads,
                Err(err) => return Box::new(future::err(err)),
            };

            if quads.len() == 0 {
                Box::new(future::ok(None))
            } else {
                let mut hash = HashMap::new();
                hash.insert("@default".to_owned(), quads);
                let jval = rdf_to_jsonld(hash, true, false);
                Box::new(future::ok(Some(StoreItem::parse(&path, jval).unwrap())))
            }
        }
    }

    fn put(&mut self, path: String, item: StoreItem) -> Self::StoreFuture {
        {
            let mut cache = self.cache.borrow_mut();
            cache.remove(&path);
        }

        let jld = item.to_json();

        let rdf = match jsonld_to_rdf(jld, &mut DefaultNodeGenerator::new()) {
            Ok(rdf) => rdf,
            Err(err) => panic!("welp {}", err),
        };

        let quads = rdf.clone().remove("@default").unwrap();
        if let Err(err) = self.write_quads(&path, quads) {
            return Box::new(future::err(err));
        }

        Box::new(future::ok(
            StoreItem::parse(&path, rdf_to_jsonld(rdf, true, false)).unwrap(),
        ))
    }

    fn read_collection(
        &self,
        path: String,
        count: Option<u32>,
        cursor: Option<String>,
    ) -> Self::ReadCollectionFuture {
        let path_id = match self.get_attribute_id(&path) {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };

        let mut result = CollectionPointer {
            items: Vec::new(),
            before: None,
            after: None,
            count: None,
        };

        // !!!! before and after are as in previous and next page respectively!!
        let mut before = i32::min_value();
        let mut after = i32::max_value();
        if let Some(cursor) = cursor {
            let spl: Vec<_> = cursor.split('-').collect();
            if spl.len() == 2 {
                if let Some(val) = spl[1].parse::<i32>().ok() {
                    if spl[0] == "before" {
                        before = val;
                    } else if spl[0] == "after" {
                        after = val;
                    }
                }
            }
        }

        use models::CollectionItem;
        use schema::collection_item::dsl::*;

        let count = count.unwrap_or(20u32);

        let items: Vec<CollectionItem> = match collection_item
            .filter(collection_id.eq(path_id))
            .filter(id.lt(after).and(id.gt(before)))
            .order(id.desc())
            .limit(count as i64)
            .load(&self.connection)
        {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };


        if items.len() > 0 {
            result.before = Some(format!("before-{}", items[0].id));
            if items.len() == count as usize {
                result.after = Some(format!("after-{}", items[(count - 1) as usize].id));
            }
        }

        let ids = items.into_iter().map(|f| f.object_id).collect();
        match self.get_attributes(&ids) {
            Ok(_) => (),
            Err(err) => return future::err(err),
        };

        let attribute_url = self.attribute_url.borrow();
        result.items = ids.into_iter().map(|f| attribute_url[&f].clone()).collect();

        future::ok(result)
    }

    fn find_collection(&self, _path: String, _item: String) -> Self::ReadCollectionFuture {
        future::ok(CollectionPointer {
            items: Vec::new(),
            before: None,
            after: None,
            count: None,
        })
    }

    fn insert_collection(&mut self, path: String, item: String) -> Self::WriteCollectionFuture {
        use models::InsertableCollectionItem;
        use schema::collection_item::dsl::*;

        let path_id = match self.get_attribute_id(&path) {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };

        let item_id = match self.get_attribute_id(&item) {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };

        insert_into(collection_item)
            .values(&InsertableCollectionItem {
                collection_id: path_id,
                object_id: item_id,
            })
            .execute(&self.connection)
            .map(|_| ())
            .into()
    }

    fn remove_collection(&mut self, path: String, item: String) -> Self::WriteCollectionFuture {
        use schema::collection_item::dsl::*;

        let path_id = match self.get_attribute_id(&path) {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };

        let item_id = match self.get_attribute_id(&item) {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };

        delete(collection_item.filter(collection_id.eq(path_id).and(object_id.eq(item_id))))
            .execute(&self.connection)
            .map(|_| ())
            .into()
    }
}
