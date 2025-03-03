DROP MATERIALIZED VIEW IF EXISTS sul_instance_ids
;
create materialized view sul_instance_ids as
select distinct(instanceid) from sul_mod_inventory_storage.holdings_record
where  permanentlocationid in (
    select id from sul_mod_inventory_storage.location
    where campusid in (
        'b89563c5-cb66-4de7-b63c-ca4d82e9d856',
        'be6468b8-ed88-4876-93fe-5bdac764959c',
        '7003123d-ef65-45f6-b469-d2b9839e1bb3',
        'c365047a-51f2-45ce-8601-e421ca3615c5'
    )
);