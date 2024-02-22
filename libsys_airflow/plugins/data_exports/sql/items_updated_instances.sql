select id
from sul_mod_inventory_storage.instance
where id in (
  select (jsonb->>'instanceId')::uuid
  from sul_mod_inventory_storage.holdings_record
  where id in (
    select (jsonb->>'holdingsRecordId')::uuid
    from sul_mod_inventory_storage.item
    where jsonb->'metadata'->>'updatedDate' between '2024-02-20' and '2024-02-21'
  )
)
and (jsonb->>'discoverySuppress')::boolean is false
and jsonb->>'catalogedDate' similar to '\d{4}-\d{2}-\d{2}'
and (jsonb->>'statusId')::uuid in (
  select id
  from sul_mod_inventory_storage.instance_status
  where jsonb->>'name' = 'Cataloged'
);