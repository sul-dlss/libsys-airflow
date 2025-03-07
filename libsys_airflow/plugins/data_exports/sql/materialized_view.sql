DROP MATERIALIZED VIEW IF EXISTS data_export_marc
;
create materialized view data_export_marc as
select I.id, I.jsonb->'hrid' as hrid, M.content
from sul_mod_inventory_storage.instance I
inner join(
  select distinct on (external_id) external_id, id, generation
  from sul_mod_source_record_storage.records_lb
  order by external_id, generation desc
) R
on R.external_id = I.id
and (I.jsonb->>'statusId')::uuid in (
    select id
    from sul_mod_inventory_storage.instance_status
    where jsonb->>'name' = 'Cataloged'
  )
  and I.jsonb->>'catalogedDate' between %(from_date)s and %(to_date)s
  and (I.jsonb->>'discoverySuppress')::boolean is false
  and I.jsonb->>'source' = 'MARC'
join sul_mod_source_record_storage.marc_records_lb M
  on M.id = R.id
right join filter_instance_ids F
  on F.instanceid = I.id
order by I.jsonb->'hrid'
;
CREATE UNIQUE INDEX data_export_marc_ids ON data_export_marc (id);
CREATE UNIQUE INDEX data_export_marc_hrids ON data_export_marc (hrid);
