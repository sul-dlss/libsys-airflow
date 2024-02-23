select id
from sul_mod_inventory_storage.instance
where jsonb->>'catalogedDate' similar to '\d{4}-\d{2}-\d{2}'
and (jsonb->>'discoverySuppress')::boolean is true;