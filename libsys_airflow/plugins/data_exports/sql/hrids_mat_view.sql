-- Materialized view built from flat lists of FOLIO instance hrids.
-- The COPY commands below require the txt files to exist on the DB server filesystem
-- and the connecting user to have pg_read_server_files privilege (or be a superuser).
-- Update the paths below to match the actual location on the DB server.

DROP TABLE IF EXISTS public.hrid_export_list;
CREATE TABLE public.hrid_export_list (hrid text);
COPY public.hrid_export_list FROM '/home/folio/hrids.txt';

DELETE FROM public.hrid_export_list a
    USING public.hrid_export_list b
    WHERE a.ctid > b.ctid AND a.hrid = b.hrid;

CREATE INDEX ON public.hrid_export_list (hrid);

DROP MATERIALIZED VIEW IF EXISTS hrids_mat_view;
CREATE MATERIALIZED VIEW hrids_mat_view AS
SELECT I.id AS instanceid,
       I.jsonb -> 'hrid' AS hrid,
       M.content
FROM sul_mod_inventory_storage.instance I
INNER JOIN public.hrid_export_list H ON I.jsonb ->> 'hrid' = H.hrid
LEFT JOIN (
    SELECT DISTINCT ON (external_id) external_id, id, generation
    FROM sul_mod_source_record_storage.records_lb
    ORDER BY external_id, generation DESC
) R ON R.external_id = I.id
LEFT JOIN sul_mod_source_record_storage.marc_records_lb M
    ON M.id = R.id
ORDER BY I.jsonb -> 'hrid';

DROP INDEX IF EXISTS hrids_mat_view_ids;
DROP INDEX IF EXISTS hrids_mat_view_hrids;
CREATE UNIQUE INDEX hrids_mat_view_ids ON hrids_mat_view (instanceid);
CREATE UNIQUE INDEX hrids_mat_view_hrids ON hrids_mat_view (hrid);
