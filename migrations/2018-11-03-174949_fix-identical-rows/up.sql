

delete from collection_item where collection_item.id not in
    (select distinct on (collection_id, object_id) id from collection_item);

alter table collection_item add constraint collection_items_unique unique (collection_id, object_id);