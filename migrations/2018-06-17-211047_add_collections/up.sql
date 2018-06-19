CREATE TABLE collection_item (
    id SERIAL PRIMARY KEY,

    collection_id integer not null references attribute,
    object_id integer not null references attribute
);

CREATE INDEX collection_item_collection on collection_item (collection_id);