DROP MATERIALIZED VIEW IF EXISTS google_mat_view;

CREATE MATERIALIZED VIEW google_mat_view AS
SELECT
    F.instanceid,
    I.jsonb ->> 'hrid' AS hrid,
    M.content
FROM filter_campus_ids F
INNER JOIN sul_mod_inventory_storage.instance I
    ON F.instanceid = I.id
    AND ((I.jsonb->>'discoverySuppress')::boolean IS FALSE OR (I.jsonb->>'discoverySuppress') IS NULL)
LEFT JOIN (
    SELECT DISTINCT ON (instanceid)
        id,
        instanceid
    FROM sul_mod_inventory_storage.holdings_record
    ORDER BY instanceid, id  -- id as tiebreaker; swap for a meaningful column if preferred
) H
    ON H.instanceid = I.id
LEFT JOIN (
    SELECT DISTINCT ON (holdingsrecordid)
        id,
        holdingsrecordid,
        materialtypeid,
        jsonb
    FROM sul_mod_inventory_storage.item T
    WHERE
        ((T.jsonb->>'discoverySuppress')::boolean IS FALSE OR (T.jsonb->>'discoverySuppress') IS NULL)
        AND T.jsonb -> 'status' ->> 'name' NOT IN ('On order')
        AND (
            T.materialtypeid IN (
                SELECT id FROM sul_mod_inventory_storage.material_type
                WHERE jsonb->>'name' IN ('book', 'periodical')
            )
            OR (
                T.materialtypeid IN (
                    SELECT id FROM sul_mod_inventory_storage.material_type
                    WHERE jsonb->>'code' IN ('score')
                )
                AND T.jsonb ->> 'numberOfPieces' = '1'
            )
        )
    ORDER BY holdingsrecordid, id  -- id as tiebreaker
) T
    ON T.holdingsrecordid = H.id
LEFT JOIN (
    SELECT DISTINCT ON (external_id)
        external_id,
        id,
        generation
    FROM sul_mod_source_record_storage.records_lb
    ORDER BY external_id, generation DESC
) R
    ON R.external_id = I.id
LEFT JOIN sul_mod_source_record_storage.marc_records_lb M
    ON M.id = R.id;

DROP INDEX IF EXISTS google_mat_view_ids;
DROP INDEX IF EXISTS google_mat_view_hrids;
CREATE UNIQUE INDEX google_mat_view_ids ON google_mat_view (instanceid);
CREATE UNIQUE INDEX google_mat_view_hrids ON google_mat_view (hrid);
