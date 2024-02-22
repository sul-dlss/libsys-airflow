select id
from sul_mod_inventory_storage.instance
where id in (
  select external_id
  from sul_mod_source_record_storage.records_lb
  where record_type = 'MARC_BIB'
  and generation > 0
  and state = 'ACTUAL'
  and created_date > '2024-02-20'
)
and (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged'
)
and jsonb->>'catalogedDate' similar to '\d{4}-\d{2}-\d{2}'
and (jsonb->>'discoverySuppress')::boolean is false;