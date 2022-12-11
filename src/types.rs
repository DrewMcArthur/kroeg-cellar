use postgres_async::types::Row;

pub struct CollectionItem {
    pub id: i32,
    pub collection_id: i32,
    pub object_id: i32,
}

impl CollectionItem {
    pub fn make_from_row(row: &Row) -> CollectionItem {
        CollectionItem {
            id: row.get(0).unwrap().unwrap(),
            collection_id: row.get(1).unwrap().unwrap(),
            object_id: row.get(2).unwrap().unwrap(),
        }
    }
}
