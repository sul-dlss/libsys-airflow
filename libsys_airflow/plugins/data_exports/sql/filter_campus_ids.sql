DROP MATERIALIZED VIEW IF EXISTS filter_campus_ids CASCADE; 
create materialized view filter_campus_ids as 
select distinct(instanceid) from sul_mod_inventory_storage.holdings_record 
where  permanentlocationid in ( 
    select id from sul_mod_inventory_storage.location 
    where campusid in ( 
        select id from sul_mod_inventory_storage.loccampus 
        where jsonb->>'code' in %(campuses)s
    )
);
DROP INDEX IF EXISTS filter_full_dump_campus_idx; 
CREATE UNIQUE INDEX filter_full_dump_campus_idx ON filter_campus_ids (instanceid);
