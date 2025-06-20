DROP MATERIALIZED VIEW IF EXISTS sal3_items;
create materialized view sal3_items as
select
    I.id as item_uuid,
    H.id as holding_id,
    I.jsonb ->> 'barcode' as item_barcode,
    I.jsonb -> 'status' ->> 'name' as item_status,
    I.permanentLocationId as item_permanent_location,
    I.temporaryLocationId as item_temporary_location,
    H.permanentLocationId as holdings_permanent_location,
    I.discoverySuppress as item_suppressed
from sul_mod_inventory_storage.holdings_record H
full outer join sul_mod_inventory_storage.item I
on H.id = I.holdingsrecordid
where (
        H.permanentlocationid in (
                select id from sul_mod_inventory_storage.location where jsonb->>'code' like '%SAL3%'
        )
        or
        I.permanentLocationId in (
                select id from sul_mod_inventory_storage.location where jsonb->>'code' like '%SAL3%'
        )
)
;
