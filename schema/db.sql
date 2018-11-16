CREATE TABLE attribute (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL
);

CREATE TABLE quad (
    id SERIAL PRIMARY KEY,
    quad_id integer not null references attribute,
    subject_id integer not null references attribute,
    predicate_id integer not null references attribute,

    -- either
    attribute_id integer null references attribute,

    -- or
    object text null,
    type_id integer null references attribute,
    language text null
);

CREATE TABLE collection_item (
    id SERIAL PRIMARY KEY,

    collection_id integer not null references attribute,
    object_id integer not null references attribute,

    constraint collection_items_unique unique (collection_id, object_id)
);

CREATE TABLE queue_item (
    id SERIAL PRIMARY KEY,
    event text not null,
    data text not null
);

CREATE INDEX attribute_url on attribute (url);
CREATE INDEX quad_quad_id on quad (quad_id);
CREATE INDEX collection_item_collection on collection_item (collection_id);