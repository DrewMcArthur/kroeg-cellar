use schema::*;

#[derive(Queryable, Debug)]
pub struct Attribute {
    pub id: i32,
    pub url: String,
}

#[derive(Insertable, Debug)]
#[table_name = "attribute"]
pub struct NewAttribute<'a> {
    pub url: &'a str,
}

#[derive(Queryable, Debug)]
pub struct Quad {
    pub id: i32,
    pub quad_id: i32,
    pub subject_id: i32,
    pub predicate_id: i32,

    pub attribute_id: Option<i32>,

    pub object: Option<String>,
    pub type_id: Option<i32>,
    pub language: Option<String>,
}

#[derive(Insertable, Debug)]
#[table_name = "quad"]
pub struct InsertableQuad {
    pub quad_id: i32,
    pub subject_id: i32,
    pub predicate_id: i32,

    pub attribute_id: Option<i32>,

    pub object: Option<String>,
    pub type_id: Option<i32>,
    pub language: Option<String>,
}

#[derive(Queryable, Debug)]
pub struct CollectionItem {
    pub id: i32,

    pub collection_id: i32,
    pub object_id: i32,
}

#[derive(Insertable, Debug)]
#[table_name = "collection_item"]
pub struct InsertableCollectionItem {
    pub collection_id: i32,
    pub object_id: i32,
}
