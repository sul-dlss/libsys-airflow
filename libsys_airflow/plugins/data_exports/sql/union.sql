select count(*)
from sul_mod_inventory_storage.instance
where (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged'
)
and jsonb->>'catalogedDate' is not null
and (jsonb->>'discoverySuppress')::boolean is false
and jsonb->>'source' = 'MARC'
;