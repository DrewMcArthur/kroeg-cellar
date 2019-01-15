use crate::CellarEntityStore;
use futures::{future, stream, Future, Stream};
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld, QuadContents, StringQuad};
use kroeg_tap::StoreItemNodeGenerator;
use kroeg_tap::{CollectionPointer, EntityStore, QuadQuery, QueryId, QueryObject, StoreItem};
use serde_json::Value as JValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio_postgres::error::Error;
use tokio_postgres::types::ToSql;

fn get_ids(quad: &StringQuad, set: &mut HashSet<String>) {
    match &quad.contents {
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
    type GetFuture = Box<Future<Item = (Option<StoreItem>, Self), Error = (Error, Self)> + Send>;

    /// Gets a single `StoreItem` from the store. Missing entities are no error,
    /// but instead returns a `None`.
    fn get(self, path: String, _local: bool) -> Self::GetFuture {
        let id = path.to_owned();

        Box::new(
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
                }),
        )
    }

    // ---

    /// The `Future` that is returned when `put`ting a `StoreItem`.
    type StoreFuture = Box<Future<Item = (StoreItem, Self), Error = (Error, Self)> + Send>;

    /// Stores a single `StoreItem` into the store.
    ///
    /// To delete an Entity, set its type to as:Tombstone. This may
    /// instantly remove it, or queue it for possible future deletion.
    fn put(mut self, path: String, item: StoreItem) -> Self::StoreFuture {
        let rdf = item.clone().to_json();

        let mut rdf = jsonld_to_rdf(rdf, &mut StoreItemNodeGenerator::new()).unwrap();
        let mut set = HashSet::new();

        let quads = rdf.remove("@default").unwrap();
        for quad in &quads {
            get_ids(quad, &mut set);
        }

        set.insert(path.to_owned());

        let set: Vec<_> = set.into_iter().collect();
        Box::new(
            self.cache_uris(&set)
                .and_then(move |store| {
                    // (quad_id, subject_id, predicate_id, attribute_id, object, type_id, language)
                    let mut quad_id = Vec::with_capacity(quads.len());
                    let mut subject_id = Vec::with_capacity(quads.len());
                    let mut predicate_id = Vec::with_capacity(quads.len());
                    let mut attribute_id = Vec::with_capacity(quads.len());
                    let mut object = Vec::with_capacity(quads.len());
                    let mut type_id = Vec::with_capacity(quads.len());
                    let mut language = Vec::with_capacity(quads.len());

                    let qid = store.cache.uri_to_id[&path];

                    for quad in quads {
                        quad_id.push(qid);
                        subject_id.push(store.cache.uri_to_id[&quad.subject_id]);
                        predicate_id.push(store.cache.uri_to_id[&quad.predicate_id]);

                        match quad.contents {
                            QuadContents::Id(id) => {
                                attribute_id.push(Some(store.cache.uri_to_id[&id]));
                                object.push(None);
                                type_id.push(None);
                                language.push(None);
                            }

                            QuadContents::Object(typ_id, content, languag) => {
                                attribute_id.push(None);
                                object.push(Some(content));
                                type_id.push(Some(store.cache.uri_to_id[&typ_id]));
                                language.push(languag);
                            }
                        }
                    }
                    store.delete_quad(qid).and_then(move |(count, store)| {
                        store.insert_quad(&[
                            &quad_id as &ToSql,
                            &subject_id,
                            &predicate_id,
                            &attribute_id,
                            &object,
                            &type_id,
                            &language,
                        ])
                    })
                })
                .map(move |(_, store)| (item, store)),
        )
    }

    // -----

    /// The `Future` that is returned when querying the database.
    type QueryFuture = Box<Future<Item = (Vec<Vec<String>>, Self), Error = (Error, Self)> + Send>;

    /// Queries the entire store for a specific set of parameters.
    /// The return value is a list for every result in the database that matches the query.
    /// The array elements are in numeric order of the placeholders.
    fn query(self, query: Vec<QuadQuery>) -> Self::QueryFuture {
        // stores a map of statements (e.g. quad_1.quad_id) and what they should be equal to. (e.g. quad_1.quad_id -> [quad_2.attribute_id])
        let mut placeholders = BTreeMap::new();

        // stores a map of attribute value and the IDs they should be equal to (e.g. quad_1.predicate_id -> https://www.w3.org/ns/activitystreams#object)
        let mut checks = HashMap::new();

        // same as checks, but for being equal to any of a list.
        let mut checks_any = HashMap::new();

        // stores statement -> string value comparisons, for contents/language tests
        let mut others = Vec::new();

        let quad_count = query.len();

        for (i, QuadQuery(subject, predicate, object)) in query.into_iter().enumerate() {
            match subject {
                QueryId::Value(val) => {
                    checks.insert(format!("quad_{}.quad_id", i), val);
                }
                QueryId::Placeholder(val) => {
                    if !placeholders.contains_key(&val) {
                        placeholders.insert(val, vec![format!("quad_{}.quad_id", i)]);
                    } else {
                        placeholders
                            .get_mut(&val)
                            .unwrap()
                            .push(format!("quad_{}.quad_id", i));
                    }
                }
                QueryId::Any(any) => {
                    if any.len() == 0 {
                        return Box::new(future::ok((vec![], self)));
                    }

                    checks_any.insert(format!("quad_{}.quad_id", i), any);
                }
                QueryId::Ignore => {}
            }
            match predicate {
                QueryId::Value(val) => {
                    checks.insert(format!("quad_{}.predicate_id", i), val);
                }
                QueryId::Placeholder(val) => {
                    if !placeholders.contains_key(&val) {
                        placeholders.insert(val, vec![format!("quad_{}.predicate_id", i)]);
                    } else {
                        placeholders
                            .get_mut(&val)
                            .unwrap()
                            .push(format!("quad_{}.predicate_id", i));
                    }
                }
                QueryId::Any(any) => {
                    if any.len() == 0 {
                        return Box::new(future::ok((vec![], self)));
                    }

                    checks_any.insert(format!("quad_{}.predicate_id", i), any);
                }
                QueryId::Ignore => {}
            }

            match object {
                QueryObject::Id(QueryId::Value(val)) => {
                    checks.insert(format!("quad_{}.attribute_id", i), val);
                }
                QueryObject::Id(QueryId::Placeholder(val)) => {
                    if !placeholders.contains_key(&val) {
                        placeholders.insert(val, vec![format!("quad_{}.attribute_id", i)]);
                    } else {
                        placeholders
                            .get_mut(&val)
                            .unwrap()
                            .push(format!("quad_{}.attribute_id", i));
                    }
                }

                QueryObject::Id(QueryId::Any(any)) => {
                    checks_any.insert(format!("quad_{}.attribute_id", i), any);
                }

                QueryObject::Id(QueryId::Ignore) => {}
                QueryObject::Object { value, type_id } => {
                    others.push((format!("quad_{}.object", i), value));
                    match type_id {
                        QueryId::Value(val) => {
                            checks.insert(format!("quad_{}.type_id", i), val);
                        }
                        QueryId::Placeholder(val) => {
                            if !placeholders.contains_key(&val) {
                                placeholders.insert(val, vec![format!("quad_{}.type_id", i)]);
                            } else {
                                placeholders
                                    .get_mut(&val)
                                    .unwrap()
                                    .push(format!("quad_{}.type_id", i));
                            }
                        }
                        QueryId::Any(any) => {
                            if any.len() == 0 {
                                return Box::new(future::ok((vec![], self)));
                            }

                            checks_any.insert(format!("quad_{}.type_id", i), any);
                        }
                        QueryId::Ignore => {}
                    }
                }
                QueryObject::LanguageString { value, language } => {
                    others.push((format!("quad_{}.object", i), value.to_owned()));
                    others.push((format!("quad_{}.language", i), language));
                }
            }
        }

        let all_attributes = checks
            .iter()
            .map(|(_, b)| b.to_owned())
            .chain(
                checks_any
                    .iter()
                    .flat_map(|(_, b)| b.iter().map(|f| f.to_string())),
            )
            .collect::<Vec<_>>();

        Box::new(
            self.cache_uris(&all_attributes)
                .and_then(move |store| {
                    let mut query = String::from("select ");
                    for (i, (_, placeholder)) in placeholders.iter().enumerate() {
                        if i != 0 {
                            query += ", ";
                        }

                        query += &placeholder[0];
                    }

                    query += " from ";

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

                    for (a, b) in checks {
                        let against = store.cache.uri_to_id[&b];

                        query += &format!(" and {} = {}", a, against);
                    }

                    for (a, b) in checks_any {
                        query += &format!(
                            "and {} in ({})",
                            a,
                            b.into_iter()
                                .map(|f| store.cache.uri_to_id[&f].to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                    }

                    // ok, query built. now send it off
                    store.do_query(query, vec![])
                })
                .and_then(move |(result, store)| {
                    let mut data = vec![];
                    for row in result {
                        let mut row_out = Vec::with_capacity(row.len());
                        for i in 0..row.len() {
                            row_out.push(row.get::<usize, i32>(i));
                        }

                        data.push(row_out);
                    }

                    store
                        .cache_ids(&data.iter().flatten().map(|f| *f).collect::<Vec<_>>())
                        .map(|store| (data, store))
                })
                .map(move |(data, store)| {
                    (
                        data.into_iter()
                            .map(|f| {
                                f.into_iter()
                                    .map(|f| store.cache.id_to_uri[&f].to_owned())
                                    .collect()
                            })
                            .collect(),
                        store,
                    )
                }),
        )
    }

    // -----

    /// The `Future` that is returned when reading the collection data.
    type ReadCollectionFuture =
        Box<Future<Item = (CollectionPointer, Self), Error = (Error, Self)> + Send>;

    /// Reads N amount of items from the collection corresponding to a specific ID. If a cursor is passed,
    /// it can be used to paginate.
    fn read_collection(
        self,
        path: String,
        count: Option<u32>,
        cursor: Option<String>,
    ) -> Self::ReadCollectionFuture {
        let (until, offset) = match cursor {
            None => (true, i32::max_value()),
            Some(ref value) if value.starts_with("before-") => match value[7..].parse::<i32>() {
                Ok(val) => (true, val - 1),
                Err(e) => {
                    return Box::new(future::ok((
                        CollectionPointer {
                            items: vec![],
                            after: None,
                            before: None,
                            count: None,
                        },
                        self,
                    )));
                }
            },
            Some(ref value) if value.starts_with("after-") => match value[7..].parse::<i32>() {
                Ok(val) => (false, val + 1),
                Err(e) => {
                    return Box::new(future::ok((
                        CollectionPointer {
                            items: vec![],
                            after: None,
                            before: None,
                            count: None,
                        },
                        self,
                    )));
                }
            },

            _ => {
                return Box::new(future::ok((
                    CollectionPointer {
                        items: vec![],
                        after: None,
                        before: None,
                        count: None,
                    },
                    self,
                )));
            }
        };

        Box::new(
            self.cache_uris(&[path.to_owned()])
                .and_then(move |store| {
                    let id = store.cache.uri_to_id[&path];
                    store.select_collection(id, offset, count.unwrap_or(30) as i32, until)
                })
                .and_then(move |(items, store)| {
                    let mut ids = Vec::new();
                    for item in &items {
                        ids.extend_from_slice(&[item.collection_id, item.object_id]);
                    }

                    store.cache_ids(&ids).map(move |store| (items, store))
                })
                .map(move |(items, store)| {
                    (
                        CollectionPointer {
                            items: items
                                .iter()
                                .map(|f| store.cache.id_to_uri[&f.object_id].to_owned())
                                .collect(),
                            after: items
                                .iter()
                                .next()
                                .map(|f| f.id)
                                .or(offset.checked_add(1))
                                .map(|val| format!("after-{}", val)),
                            before: items
                                .iter()
                                .last()
                                .map(|f| f.id)
                                .or(offset.checked_sub(1))
                                .map(|val| format!("before-{}", val)),
                            count: None,
                        },
                        store,
                    )
                }),
        )
    }

    // -----

    type FindCollectionFuture =
        Box<Future<Item = (CollectionPointer, Self), Error = (Error, Self)> + Send>;

    /// Finds an item in a collection. The result will contain cursors to just before and after the item, if it exists.
    fn find_collection(self, path: String, item: String) -> Self::FindCollectionFuture {
        Box::new(
            self.cache_uris(&[path.to_owned(), item.to_owned()])
                .and_then(move |store| {
                    let path_id = store.cache.uri_to_id[&path];
                    let item_id = store.cache.uri_to_id[&item];

                    store
                        .collection_contains(path_id, item_id)
                        .map(move |(exists, store)| (exists, store, item_id, item))
                })
                .map(move |(exists, store, item_id, item)| {
                    if exists {
                        (
                            CollectionPointer {
                                items: vec![item],
                                after: item_id.checked_add(1).map(|val| format!("after-{}", val)),
                                before: item_id.checked_sub(1).map(|val| format!("before-{}", val)),
                                count: None,
                            },
                            store,
                        )
                    } else {
                        (
                            CollectionPointer {
                                items: vec![],
                                after: None,
                                before: None,
                                count: None,
                            },
                            store,
                        )
                    }
                }),
        )
    }

    // -----

    /// The `Future` that is returned when writing into a collection.
    type WriteCollectionFuture = Box<Future<Item = Self, Error = (Error, Self)> + Send>;

    /// Inserts an item into the back of the collection.
    fn insert_collection(self, path: String, item: String) -> Self::WriteCollectionFuture {
        Box::new(
            self.cache_uris(&[path.to_owned(), item.to_owned()])
                .and_then(move |store| {
                    let path = store.cache.uri_to_id[&path];
                    let item = store.cache.uri_to_id[&item];
                    store.insert_collection(path, item)
                }),
        )
    }

    // -----

    type ReadCollectionInverseFuture =
        Box<Future<Item = (CollectionPointer, Self), Error = (Error, Self)> + Send>;

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

    type RemoveCollectionFuture = Box<Future<Item = Self, Error = (Error, Self)> + Send>;

    /// Removes an item from the collection.
    fn remove_collection(self, path: String, item: String) -> Self::RemoveCollectionFuture {
        Box::new(
            self.cache_uris(&[path.to_owned(), item.to_owned()])
                .and_then(move |store| {
                    let path = store.cache.uri_to_id[&path];
                    let item = store.cache.uri_to_id[&item];
                    store.delete_collection(path, item)
                }),
        )
    }
}
