use super::QuadClient;

use diesel::prelude::*;
use diesel::result::Error;
use diesel::{delete, insert_into};

use futures::future;
use futures::prelude::*;

use std::collections::HashMap;

use jsonld::nodemap::DefaultNodeGenerator;
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld};
use kroeg_tap::{CollectionPointer, EntityStore, QueueItem, QueueStore, StoreItem};
use serde_json::Value as JValue;

use super::models;

impl QueueItem for models::QueueItem {
    fn event(&self) -> &str {
        &self.event
    }

    fn data(&self) -> &str {
        &self.data
    }
}

impl QueueStore for QuadClient {
    type Item = models::QueueItem;
    type Error = Error;
    type GetItemFuture = Box<Future<Item = Option<Self::Item>, Error = Self::Error> + Send + Sync>;
    type MarkFuture = Box<Future<Item = (), Error = Self::Error> + Send + Sync>;

    fn get_item(&self) -> Self::GetItemFuture {
        use models::QueueItem;
        use schema::queue_item::dsl::*;

        match queue_item
            .order(id)
            .limit(1)
            .get_result::<QueueItem>(&self.connection)
            .optional()
        {
            Ok(Some(val)) => {
                match delete(queue_item.filter(id.eq(val.id))).execute(&self.connection) {
                    Ok(_) => Box::new(future::ok(Some(val))),
                    Ok(0) => Box::new(future::ok(None)),
                    Err(e) => return Box::new(future::err(e)),
                }
            }
            Ok(None) => Box::new(future::ok(None)),
            Err(e) => Box::new(future::err(e)),
        }
    }

    fn mark_success(&self, _item: models::QueueItem) -> Self::MarkFuture {
        Box::new(future::ok(()))
    }

    fn mark_failure(&self, item: models::QueueItem) -> Self::MarkFuture {
        use models::InsertableQueueItem;
        use schema::queue_item::dsl::*;

        match insert_into(queue_item)
            .values(&InsertableQueueItem {
                event: item.event,
                data: item.data,
            }).execute(&self.connection)
        {
            Ok(_) => Box::new(future::ok(())),
            Err(e) => Box::new(future::err(e)),
        }
    }

    fn add(&self, event: String, data: String) -> Self::MarkFuture {
        use models::InsertableQueueItem;
        use schema::queue_item::dsl::queue_item;

        match insert_into(queue_item)
            .values(&InsertableQueueItem {
                event: event,
                data: data,
            }).execute(&self.connection)
        {
            Ok(_) => Box::new(future::ok(())),
            Err(e) => Box::new(future::err(e)),
        }
    }
}

impl EntityStore for QuadClient {
    type Error = Error;
    type GetFuture = Box<Future<Item = Option<StoreItem>, Error = Self::Error> + Send + Sync>;
    type StoreFuture = Box<Future<Item = StoreItem, Error = Self::Error> + Send + Sync>;

    type ReadCollectionFuture = future::FutureResult<CollectionPointer, Self::Error>;
    type WriteCollectionFuture = future::FutureResult<(), Self::Error>;

    fn get(&self, path: String, _local: bool) -> Self::GetFuture {
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
                match rdf_to_jsonld(hash, true, false) {
                    JValue::Object(jval) => {
                        let jval = JValue::Array(jval.into_iter().map(|(_, b)| b).collect());
                        Box::new(future::ok(Some(StoreItem::parse(&path, jval).unwrap())))
                    }
                    _ => unreachable!(),
                }
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

        let count = count.unwrap_or(50u32);

        let mut items: Vec<CollectionItem> = match if before == i32::min_value() {
            collection_item
                .filter(collection_id.eq(path_id))
                .filter(id.lt(after).and(id.gt(before)))
                .order(id.desc())
                .limit(count as i64)
                .load(&self.connection)
        } else {
            collection_item
                .filter(collection_id.eq(path_id))
                .filter(id.lt(after).and(id.gt(before)))
                .order(id.asc())
                .limit(count as i64)
                .load(&self.connection)
        } {
            Ok(ok) => ok,
            Err(err) => return future::err(err),
        };

        items.sort_by_key(|f| -f.id);

        if before != i32::min_value() {
            result.after = Some(format!("after-{}", before));
        }

        if after != i32::max_value() {
            result.before = Some(format!("before-{}", after));
        }

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
            }).execute(&self.connection)
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
