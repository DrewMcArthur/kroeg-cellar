use super::QuadClient;

use diesel::expression::dsl::sql;
use diesel::pg::types::sql_types::Array;
use diesel::prelude::*;
use diesel::result::Error;
use diesel::types::Integer;
use diesel::{delete, insert_into};

use futures::future;
use futures::prelude::*;

use std::collections::{BTreeMap, HashMap};

use jsonld::nodemap::DefaultNodeGenerator;
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld};
use kroeg_tap::{
    CollectionPointer, EntityStore, QuadQuery, QueryId, QueryObject, QueueItem, QueueStore,
    StoreItem,
};
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
                    Ok(0) => Box::new(future::ok(None)),
                    Ok(_) => Box::new(future::ok(Some(val))),
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

    type QueryFuture = future::FutureResult<Vec<Vec<String>>, Self::Error>;

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

    fn query(&self, data: Vec<QuadQuery>) -> Self::QueryFuture {
        let mut placeholders = BTreeMap::new();
        let mut checks = HashMap::new();
        let mut others = Vec::new();
        let quad_count = data.len();

        for (i, QuadQuery(subject, predicate, object)) in data.into_iter().enumerate() {
            match subject {
                QueryId::Value(val) => {
                    checks.insert(format!("quad_{}.quad_id", i), val);
                }
                QueryId::Placeholder(val) => if !placeholders.contains_key(&val) {
                    placeholders.insert(val, vec![format!("quad_{}.quad_id", i)]);
                } else {
                    placeholders
                        .get_mut(&val)
                        .unwrap()
                        .push(format!("quad_{}.quad_id", i));
                },
                QueryId::Ignore => {}
            }
            match predicate {
                QueryId::Value(val) => {
                    checks.insert(format!("quad_{}.predicate_id", i), val);
                }
                QueryId::Placeholder(val) => if !placeholders.contains_key(&val) {
                    placeholders.insert(val, vec![format!("quad_{}.predicate_id", i)]);
                } else {
                    placeholders
                        .get_mut(&val)
                        .unwrap()
                        .push(format!("quad_{}.predicate_id", i));
                },
                QueryId::Ignore => {}
            }

            match object {
                QueryObject::Id(QueryId::Value(val)) => {
                    checks.insert(format!("quad_{}.attribute_id", i), val);
                }
                QueryObject::Id(QueryId::Placeholder(val)) => if !placeholders.contains_key(&val) {
                    placeholders.insert(val, vec![format!("quad_{}.attribute_id", i)]);
                } else {
                    placeholders
                        .get_mut(&val)
                        .unwrap()
                        .push(format!("quad_{}.attribute_id", i));
                },
                QueryObject::Id(QueryId::Ignore) => {}
                QueryObject::Object { value, type_id } => {
                    others.push((format!("quad_{}.object", i), value));
                    match type_id {
                        QueryId::Value(val) => {
                            checks.insert(format!("quad_{}.type_id", i), val);
                        }
                        QueryId::Placeholder(val) => if !placeholders.contains_key(&val) {
                            placeholders.insert(val, vec![format!("quad_{}.type_id", i)]);
                        } else {
                            placeholders
                                .get_mut(&val)
                                .unwrap()
                                .push(format!("quad_{}.type_id", i));
                        },
                        QueryId::Ignore => {}
                    }
                }
                QueryObject::LanguageString { value, language } => {
                    others.push((format!("quad_{}.object", i), value.to_owned()));
                    others.push((format!("quad_{}.language", i), value));
                }
            }
        }

        let mut query = String::from("select array[");
        for (i, (_, placeholder)) in placeholders.iter().enumerate() {
            if i != 0 {
                query += ", "
            }

            query += &placeholder[0];
        }

        query += "] from ";

        for i in 0..quad_count {
            if i != 0 {
                query += ", ";
            }

            query += &format!("quad quad_{}", i);
        }

        query += " where true ";
        for (a, b) in others {
            query += &format!("and {} = '{}' ", a, b.replace("'", "''"));
        }

        for (_, placeholder) in placeholders {
            for (a, b) in placeholder.iter().zip(placeholder.iter().skip(1)) {
                query += &format!("and {} = {} ", a, b);
            }
        }

        if let Err(e) = self.store_attributes(&checks.iter().map(|(_, b)| b as &str).collect()) {
            return future::err(e);
        }

        {
            let borrowed = self.attribute_id.borrow();
            for (a, b) in checks {
                let against = borrowed[&b];

                query += &format!(" and {} = {}", a, against);
            }
        }

        let mut data: Vec<Vec<i32>> = match sql::<Array<Integer>>(&query).load(&self.connection) {
            Ok(data) => data,
            Err(e) => return future::err(e),
        };

        if let Err(e) = self.get_attributes(&data.iter().flatten().map(|f| *f).collect()) {
            return future::err(e);
        }

        let borrowed = self.attribute_url.borrow();
        future::ok(
            data.into_iter()
                .map(|f| f.into_iter().map(|f| borrowed[&f].to_owned()).collect())
                .collect(),
        )
    }
}
