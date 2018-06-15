CREATE TABLE attribute (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL
);

CREATE INDEX attribute_url on attribute (url);

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
    language char(2) null
);

CREATE INDEX quad_quad_id on quad (quad_id);