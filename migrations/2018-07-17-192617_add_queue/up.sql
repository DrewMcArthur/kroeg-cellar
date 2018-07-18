CREATE TABLE queue_item (
    id SERIAL PRIMARY KEY,
    event text not null,
    data text not null
);