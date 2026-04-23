DROP MATERIALIZED VIEW IF EXISTS google_mat_view;

CREATE MATERIALIZED VIEW google_mat_view AS
WITH allowed_material_types AS (
    SELECT id, jsonb->>'name' as name, jsonb->>'code' as code
    FROM sul_mod_inventory_storage.material_type
    WHERE jsonb->>'name' IN ('book', 'periodical')
       OR jsonb->>'code' = 'score'
),
filtered_items AS (
    SELECT DISTINCT ON (holdingsrecordid)
        T.id,
        T.holdingsrecordid,
        T.materialtypeid,
        T.jsonb
    FROM sul_mod_inventory_storage.item T
    INNER JOIN allowed_material_types MT ON T.materialtypeid = MT.id
    WHERE
        ((T.jsonb->>'discoverySuppress')::boolean IS NOT TRUE OR (T.jsonb->>'discoverySuppress') IS NULL)
        AND T.jsonb -> 'status' ->> 'name' != 'On order'
        AND (
            MT.name IN ('book', 'periodical')
            OR (MT.code = 'score' AND T.jsonb ->> 'numberOfPieces' = '1')
        )
    ORDER BY T.holdingsrecordid, T.id
)
SELECT
    F.instanceid,
    I.jsonb ->> 'hrid' AS hrid,
    M.content
FROM filtered_items T
INNER JOIN sul_mod_inventory_storage.holdings_record H 
    ON H.id = T.holdingsrecordid
INNER JOIN sul_mod_inventory_storage.instance I
    ON I.id = H.instanceid
    AND ((I.jsonb->>'discoverySuppress')::boolean IS NOT TRUE OR (I.jsonb->>'discoverySuppress') IS NULL)
INNER JOIN filter_campus_ids F
    ON F.instanceid = I.id
LEFT JOIN (
    SELECT DISTINCT ON (external_id)
        external_id,
        id
    FROM sul_mod_source_record_storage.records_lb
    ORDER BY external_id, generation DESC
) R ON R.external_id = I.id
LEFT JOIN sul_mod_source_record_storage.marc_records_lb M 
    ON M.id = R.id;

DROP INDEX IF EXISTS google_mat_view_ids;
DROP INDEX IF EXISTS google_mat_view_hrids;
CREATE UNIQUE INDEX google_mat_view_ids ON google_mat_view (instanceid);
CREATE UNIQUE INDEX google_mat_view_hrids ON google_mat_view (hrid);
