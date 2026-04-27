DROP MATERIALIZED VIEW IF EXISTS data_export_instance;
create materialized view data_export_instance as
select F.instanceid,
I.jsonb
from filter_campus_ids F
inner join sul_mod_inventory_storage.instance I
on F.instanceid = I.id
  and I.jsonb->>'catalogedDate' between %(from_date)s and %(to_date)s
  and I.jsonb->>'source' = 'MARC'
  and (I.jsonb->>'statusId')::uuid in (
    select id
    from sul_mod_inventory_storage.instance_status
    where jsonb->>'name' = 'Cataloged'
  )
  and ((I.jsonb->>'discoverySuppress')::boolean is false or (I.jsonb->>'discoverySuppress') is null)
order by I.jsonb->'hrid';
DROP INDEX IF EXISTS data_export_instance_ids;
CREATE UNIQUE INDEX data_export_instance_ids ON data_export_instance (instanceid);
