create materialized view data_export_marc as
select
  I.id,
  M.content
from sul_mod_inventory_storage.instance I
inner join sul_mod_source_record_storage.records_lb R
  on  R.external_id = I.id
  and R.state = 'ACTUAL'
  and (I.jsonb->>'statusId')::uuid in (
    select id
    from sul_mod_inventory_storage.instance_status
    where jsonb->>'name' = 'Cataloged'
  )
  and I.jsonb->>'catalogedDate' is not null
  and (I.jsonb->>'discoverySuppress')::boolean is false
  and I.jsonb->>'source' = 'MARC'
inner join sul_mod_source_record_storage.marc_records_lb M
  on M.id = R.id
;
