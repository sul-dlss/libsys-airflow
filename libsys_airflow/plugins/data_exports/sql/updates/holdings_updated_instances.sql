(select id
from sul_mod_inventory_storage.instance
where id in (
  select (jsonb->>'instanceId')::uuid
  from sul_mod_inventory_storage.holdings_record
  where jsonb->'metadata'->>'updatedDate' between %(from_date)s and %(to_date)s
)
and ((jsonb->>'discoverySuppress')::boolean is false or (jsonb->>'discoverySuppress') is null)
and jsonb->>'catalogedDate' similar to '\d{4}-\d{2}-\d{2}'
and (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged'
))