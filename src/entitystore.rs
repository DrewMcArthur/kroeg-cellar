use crate::CellarEntityStore;
use jsonld::rdf::{jsonld_to_rdf, rdf_to_jsonld, QuadContents, StringQuad};
use kroeg_tap::StoreItemNodeGenerator;
use kroeg_tap::{
    CollectionPointer, EntityStore, QuadQuery, QueryId, QueryObject, StoreError, StoreItem,
};
use serde_json::Value as JValue;
use std::collections::{BTreeMap, HashMap, HashSet};

fn get_ids(quad: &StringQuad, set: &mut HashSet<String>) {
    match &quad.contents {
        QuadContents::Id(id) => set.insert(id.to_owned()),
        QuadContents::Object(type_id, _, _) => set.insert(type_id.to_owned()),
    };

    set.insert(quad.subject_id.to_owned());
    set.insert(quad.predicate_id.to_owned());
}

#[async_trait::async_trait]
/// An entity store, storing JSON-LD `Entity` objects.
impl<'a> EntityStore for CellarEntityStore<'a> {
    /// Gets a single `StoreItem` from the store. Missing entities are no error,
    /// but instead returns a `None`.
    async fn get(&mut self, path: String, _local: bool) -> Result<Option<StoreItem>, StoreError> {
        let id = path.to_owned();

        self.cache_uris(&[path.clone()]).await?;

        let quads = self.read_quad(self.cache.uri_to_id[&path]).await?;
        let translated = self.translate_quads(quads).await?;
        if translated.is_empty() {
            return Ok(None);
        }

        let mut hash = HashMap::new();
        hash.insert("@default".to_owned(), translated);

        if let JValue::Object(jval) = rdf_to_jsonld(&hash, true, false) {
            let jval = JValue::Array(jval.into_iter().map(|(_, b)| b).collect());
            Ok(Some(StoreItem::parse(&id, &jval)?))
        } else {
            unreachable!();
        }
    }

    /// Stores a single `StoreItem` into the store.
    ///
    /// To delete an Entity, set its type to as:Tombstone. This may
    /// instantly remove it, or queue it for possible future deletion.
    async fn put(&mut self, path: String, item: &mut StoreItem) -> Result<(), StoreError> {
        let rdf = item.clone().to_json();

        let mut rdf = jsonld_to_rdf(&rdf, &mut StoreItemNodeGenerator::new()).unwrap();
        let mut set = HashSet::new();

        let quads = rdf.remove("@default").unwrap();
        for quad in &quads {
            get_ids(quad, &mut set);
        }

        set.insert(path.to_owned());

        let set: Vec<_> = set.into_iter().collect();
        self.cache_uris(&set).await?;

        let mut quad_id = Vec::with_capacity(quads.len());
        let mut subject_id = Vec::with_capacity(quads.len());
        let mut predicate_id = Vec::with_capacity(quads.len());
        let mut attribute_id = Vec::with_capacity(quads.len());
        let mut object = Vec::with_capacity(quads.len());
        let mut type_id = Vec::with_capacity(quads.len());
        let mut language = Vec::with_capacity(quads.len());

        let qid = self.cache.uri_to_id[&path];

        for quad in quads {
            quad_id.push(qid);
            subject_id.push(self.cache.uri_to_id[&quad.subject_id]);
            predicate_id.push(self.cache.uri_to_id[&quad.predicate_id]);

            match quad.contents {
                QuadContents::Id(id) => {
                    attribute_id.push(Some(self.cache.uri_to_id[&id]));
                    object.push(None);
                    type_id.push(None);
                    language.push(None);
                }

                QuadContents::Object(typ_id, content, languag) => {
                    attribute_id.push(None);
                    object.push(Some(content));
                    type_id.push(Some(self.cache.uri_to_id[&typ_id]));
                    language.push(languag);
                }
            }
        }

        self.delete_quad(qid).await?;
        self.insert_quad(&[
            &quad_id,
            &subject_id,
            &predicate_id,
            &attribute_id,
            &object,
            &type_id,
            &language,
        ])
        .await?;

        Ok(())
    }

    /// Queries the entire store for a specific set of parameters.
    /// The return value is a list for every result in the database that matches the query.
    /// The array elements are in numeric order of the placeholders.
    async fn query(&mut self, query: Vec<QuadQuery>) -> Result<Vec<Vec<String>>, StoreError> {
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
                        return Ok(vec![]);
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
                        return Ok(vec![]);
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
                    if any.len() == 0 {
                        return Ok(vec![]);
                    }

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
                                return Ok(vec![]);
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

        self.cache_uris(&all_attributes).await?;

        let mut query = String::from("select ");

        let select_count = placeholders.len();
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
            let against = self.cache.uri_to_id[&b];

            query += &format!("and {} = {} ", a, against);
        }

        for (a, b) in checks_any {
            query += &format!(
                "and {} in ({}) ",
                a,
                b.into_iter()
                    .map(|f| self.cache.uri_to_id[&f].to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        // ok, query built. now send it off
        let result = self.do_query(query, &[]).await?;

        let mut data = vec![];
        for row in result {
            let mut row_out = Vec::with_capacity(select_count);
            for i in 0..select_count {
                row_out.push(row.get::<i32>(i)?.unwrap());
            }

            data.push(row_out);
        }

        self.cache_ids(&data.iter().flatten().map(|f| *f).collect::<Vec<_>>())
            .await?;

        Ok(data
            .into_iter()
            .map(|f| {
                f.into_iter()
                    .map(|f| self.cache.id_to_uri[&f].to_owned())
                    .collect()
            })
            .collect())
    }

    /// Reads N amount of items from the collection corresponding to a specific ID. If a cursor is passed,
    /// it can be used to paginate.
    async fn read_collection(
        &mut self,
        path: String,
        count: Option<u32>,
        cursor: Option<String>,
    ) -> Result<CollectionPointer, StoreError> {
        let (until, offset) = match cursor {
            None => (true, i32::max_value()),
            Some(ref value) if value.starts_with("before-") => (false, value[7..].parse::<i32>()?),
            Some(ref value) if value.starts_with("after-") => (true, value[6..].parse::<i32>()?),
            _ => return Err("unknown collection cursor".into()),
        };

        self.cache_uris(&[path.to_owned()]).await?;
        let items = self
            .select_collection(
                self.cache.uri_to_id[&path],
                offset,
                count.unwrap_or(30) as i32,
                until,
            )
            .await?;

        let mut all_ids = Vec::new();
        for item in &items {
            all_ids.extend_from_slice(&[item.collection_id, item.object_id]);
        }

        self.cache_ids(&all_ids).await?;

        Ok(CollectionPointer {
            items: items
                .iter()
                .map(|f| self.cache.id_to_uri[&f.object_id].to_owned())
                .collect(),
            after: items
                .iter()
                .last()
                .and_then(|f| f.id.checked_sub(1))
                .or(if !until { offset.checked_sub(1) } else { None })
                .map(|var| format!("after-{}", var)),
            before: items
                .iter()
                .next()
                .and_then(|f| f.id.checked_add(1))
                .or(if until { offset.checked_add(1) } else { None })
                .map(|var| format!("before-{}", var)),
            count: None,
        })
    }

    /// Finds an item in a collection. The result will contain cursors to just before and after the item, if it exists.
    async fn find_collection(
        &mut self,
        path: String,
        item: String,
    ) -> Result<CollectionPointer, StoreError> {
        self.cache_uris(&[path.to_owned(), item.to_owned()]).await?;

        let path_id = self.cache.uri_to_id[&path];
        let item_id = self.cache.uri_to_id[&item];

        if self.collection_contains(path_id, item_id).await? {
            Ok(CollectionPointer {
                items: vec![item],
                after: item_id.checked_sub(1).map(|val| format!("after-{}", val)),
                before: item_id.checked_add(1).map(|val| format!("before-{}", val)),
                count: None,
            })
        } else {
            Ok(CollectionPointer {
                items: vec![],
                after: None,
                before: None,
                count: None,
            })
        }
    }

    /// Inserts an item into the back of the collection.
    async fn insert_collection(&mut self, path: String, item: String) -> Result<(), StoreError> {
        self.cache_uris(&[path.to_owned(), item.to_owned()]).await?;
        let path = self.cache.uri_to_id[&path];
        let item = self.cache.uri_to_id[&item];

        self.insert_collection(path, item).await
    }

    /// Finds all the collections containing a specific object.
    async fn read_collection_inverse(
        &mut self,
        item: String,
    ) -> Result<CollectionPointer, StoreError> {
        self.cache_uris(&[item.to_owned()]).await?;
        let id = self.cache.uri_to_id[&item];
        let items = self.select_collection_inverse(id).await?;

        let mut ids = Vec::new();
        for item in &items {
            ids.extend_from_slice(&[item.collection_id]);
        }

        self.cache_ids(&ids).await?;

        Ok(CollectionPointer {
            items: items
                .iter()
                .map(|f| self.cache.id_to_uri[&f.collection_id].to_owned())
                .collect(),
            after: None,
            before: None,
            count: None,
        })
    }

    /// Removes an item from the collection.
    async fn remove_collection(&mut self, path: String, item: String) -> Result<(), StoreError> {
        self.cache_uris(&[path.to_owned(), item.to_owned()]).await?;

        let path = self.cache.uri_to_id[&path];
        let item = self.cache.uri_to_id[&item];
        self.delete_collection(path, item).await
    }
}
