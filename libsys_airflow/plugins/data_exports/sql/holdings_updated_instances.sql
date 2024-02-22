select id
from sul_mod_inventory_storage.instance
where id in (
  select (jsonb->>'instanceId')::uuid
  from sul_mod_inventory_storage.holdings_record
  where jsonb->'metadata'->>'updatedDate' between %s and %s
)
and (jsonb->>'discoverySuppress')::boolean is false
and jsonb->>'catalogedDate' similar to '\d{4}-\d{2}-\d{2}'
and (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged'
);