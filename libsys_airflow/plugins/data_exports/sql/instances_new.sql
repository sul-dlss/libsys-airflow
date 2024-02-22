select id
from sul_mod_inventory_storage.instance
where (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged')
  and jsonb->>'catalogedDate' = %s
  and (jsonb->>'discoverySuppress')::boolean is false;