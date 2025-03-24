DROP MATERIALIZED VIEW IF EXISTS data_export_marc;
create materialized view data_export_marc as
select F.instanceid,
I.jsonb->'hrid' as hrid,
M.content
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
left join (
  select distinct on (external_id) external_id, id, generation
  from sul_mod_source_record_storage.records_lb
  order by external_id, generation desc
) R
on R.external_id = I.id
left join sul_mod_source_record_storage.marc_records_lb M
  on M.id = R.id
order by I.jsonb->'hrid';
DROP INDEX IF EXISTS data_export_marc_ids;
DROP INDEX IF EXISTS data_export_marc_hrids;
CREATE UNIQUE INDEX data_export_marc_ids ON data_export_marc (instanceid);
CREATE UNIQUE INDEX data_export_marc_hrids ON data_export_marc (hrid);