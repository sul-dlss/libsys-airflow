(select id
from sul_mod_inventory_storage.instance
where (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged'
)
and jsonb->>'catalogedDate' between %(from_date)s and %(to_date)s
and ((jsonb->>'discoverySuppress')::boolean is false or (jsonb->>'discoverySuppress') is null))