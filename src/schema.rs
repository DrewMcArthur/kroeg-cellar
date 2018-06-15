table! {
    attribute (id) {
        id -> Int4,
        url -> Text,
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
        language -> Nullable<Text>,
    }
}

allow_tables_to_appear_in_same_query!(attribute, quad,);
