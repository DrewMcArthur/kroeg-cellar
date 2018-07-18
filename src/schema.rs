table! {
    attribute (id) {
        id -> Int4,
        url -> Text,
    }
}

table! {
    collection_item (id) {
        id -> Int4,
        collection_id -> Int4,
        object_id -> Int4,
    }
}

table! {
    quad (id) {
        id -> Int4,
        quad_id -> Int4,
        subject_id -> Int4,
        predicate_id -> Int4,
        attribute_id -> Nullable<Int4>,
        object -> Nullable<Text>,
        type_id -> Nullable<Int4>,
        language -> Nullable<Bpchar>,
    }
}

table! {
    queue_item (id) {
        id -> Int4,
        event -> Text,
        data -> Text,
    }
}

allow_tables_to_appear_in_same_query!(attribute, collection_item, quad,);
