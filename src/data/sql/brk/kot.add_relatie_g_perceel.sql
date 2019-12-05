-- Adds 'ontstaan_uit_kadastraalobject' and 'relatie_g_perceel' values to A percelen in the KOT table.
--
-- 'ontstaan_uit_kadastraalobject' contains the direct parent(s) of an A-perceel.
-- 'relatie_g_perceel' contains the G-perceel predecessors of an A-perceel.

-- Add first order relation. Set ontstaan_uit_kadastraalobject for all A-percelen
-- Relation is derived through the ontstaan_uit_asg_id and betrokken_bij_asg_id values from 'zakelijk_recht'.
-- ASG stands for 'appartementsrechtsplitsing'. When A has the same value for 'betrokken_bij' as B for 'onstaan_uit',
-- they were involved in the same ASG. In this case A is a parent for B (there can be multiple parents).
--
-- In this query we set all direct parent <-> child relation

-- Analyze database first.
ANALYZE;

UPDATE brk_prep.kadastraal_object kot
SET ontstaan_uit_kadastraalobject=q.ontstaan_uit_kadastraalobject
FROM (
    SELECT
        kot.nrn_kot_id,
        kot.nrn_kot_volgnr,
        array_to_json(
            array_agg(
                json_build_object(
                    'brk_kot_id', ontst_uit_kot.brk_kot_id,
                    'nrn_kot_id', ontst_uit_kot.nrn_kot_id,
                    'kot_volgnummer', ontst_uit_kot.nrn_kot_volgnr
                ) ORDER BY
                    ontst_uit_kot.brk_kot_id,
                    ontst_uit_kot.nrn_kot_id,
                    ontst_uit_kot.nrn_kot_volgnr
            )
        ) AS ontstaan_uit_kadastraalobject
    FROM brk_prep.kadastraal_object kot
    LEFT JOIN brk_prep.zakelijk_recht zrt_o
        ON zrt_o.rust_op_kadastraalobject_id=kot.nrn_kot_id
        AND zrt_o.rust_op_kadastraalobj_volgnr=kot.nrn_kot_volgnr
    LEFT JOIN brk_prep.zakelijk_recht zrt_b
        ON zrt_b.betrokken_bij_asg_id=zrt_o.ontstaan_uit_asg_id
    LEFT JOIN brk_prep.kadastraal_object ontst_uit_kot
        ON zrt_b.rust_op_kadastraalobject_id=ontst_uit_kot.nrn_kot_id
        AND zrt_b.rust_op_kadastraalobj_volgnr=ontst_uit_kot.nrn_kot_volgnr
    WHERE kot.index_letter = 'A'
        AND zrt_o.ontstaan_uit_asg_id IS NOT NULL
    GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr
) q(nrn_kot_id, nrn_kot_volgnr, ontstaan_uit_kadastraalobject)
WHERE kot.ontstaan_uit_kadastraalobject = 'null'
    AND kot.index_letter = 'A'
    AND q.nrn_kot_id = kot.nrn_kot_id
    AND q.nrn_kot_volgnr = kot.nrn_kot_volgnr
;

-- Set relatie_g_perceel for all A-percelen directly related to a G-perceel
UPDATE brk_prep.kadastraal_object kot
SET relatie_g_perceel=q.relatie_g_perceel
FROM (
    SELECT
        kot.nrn_kot_id,
        kot.nrn_kot_volgnr,
        array_to_json(
            array_agg(
                json_build_object(
                    'brk_kot_id', ontst_uit_kot.brk_kot_id,
                    'nrn_kot_id', ontst_uit_kot.nrn_kot_id,
                    'kot_volgnummer', ontst_uit_kot.nrn_kot_volgnr
                ) ORDER BY
                    ontst_uit_kot.brk_kot_id,
                    ontst_uit_kot.nrn_kot_id,
                    ontst_uit_kot.nrn_kot_volgnr
            )
        ) AS relatie_g_perceel
    FROM brk_prep.kadastraal_object kot
    LEFT JOIN jsonb_array_elements(kot.ontstaan_uit_kadastraalobject) json_elms(obj)
        ON TRUE
    LEFT JOIN brk_prep.kadastraal_object ontst_uit_kot
        ON ontst_uit_kot.nrn_kot_id=(json_elms.obj->>'nrn_kot_id')::integer
        AND ontst_uit_kot.nrn_kot_volgnr=(json_elms.obj->>'kot_volgnummer')::integer
    WHERE ontst_uit_kot.index_letter='G'
        AND kot.index_letter='A'
        AND kot.relatie_g_perceel = 'null'
        AND kot.ontstaan_uit_kadastraalobject <> 'null'
    GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr
) q(nrn_kot_id, nrn_kot_volgnr, relatie_g_perceel)
WHERE kot.nrn_kot_id=q.nrn_kot_id
    AND kot.nrn_kot_volgnr=q.nrn_kot_volgnr
;

-- Set relatie_g_perceel for all other A-percelen.
-- Inherit all items in 'relatie_g_perceel' of parent objects.
CREATE OR REPLACE FUNCTION kot_derive_g_percelen() RETURNS integer AS
$$
DECLARE
    total		integer := 0;
    lastres		integer := 0;
    max_iter	integer := 20;
    iter_cnt	integer := 0;
BEGIN
    LOOP
        UPDATE brk_prep.kadastraal_object kot
        SET relatie_g_perceel=q.relatie_g_perceel
        FROM (
            SELECT
                kot.nrn_kot_id,
                kot.nrn_kot_volgnr,
                array_to_json(
                    array_agg(
                        json_build_object(
                            'brk_kot_id', gperceel.brk_kot_id,
                            'nrn_kot_id', gperceel.nrn_kot_id,
                            'kot_volgnummer', gperceel.nrn_kot_volgnr
                        ) ORDER BY
                            gperceel.brk_kot_id,
                            gperceel.nrn_kot_id,
                            gperceel.nrn_kot_volgnr
                    )
                ) AS relatie_g_perceel
            FROM brk_prep.kadastraal_object kot
            LEFT JOIN jsonb_array_elements(kot.ontstaan_uit_kadastraalobject) json_elms(obj)
                ON TRUE
            LEFT JOIN brk_prep.kadastraal_object ontst_uit_kot
                ON ontst_uit_kot.nrn_kot_id=(json_elms.obj->>'nrn_kot_id')::integer
                AND ontst_uit_kot.nrn_kot_volgnr=(json_elms.obj->>'kot_volgnummer')::integer
            LEFT JOIN jsonb_array_elements(ontst_uit_kot.relatie_g_perceel) json_gperc_elms(obj)
                ON TRUE
            LEFT JOIN brk_prep.kadastraal_object gperceel
                ON gperceel.nrn_kot_id=(json_gperc_elms.obj->>'nrn_kot_id')::integer
                AND gperceel.nrn_kot_volgnr=(json_gperc_elms.obj->>'kot_volgnummer')::integer
            WHERE ontst_uit_kot.relatie_g_perceel <> 'null'
                AND kot.index_letter = 'A'
                AND kot.relatie_g_perceel = 'null'
                AND kot.ontstaan_uit_kadastraalobject <> 'null'
            GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr
        ) q(nrn_kot_id, nrn_kot_volgnr, relatie_g_perceel)
        WHERE kot.nrn_kot_id=q.nrn_kot_id
            AND kot.nrn_kot_volgnr=q.nrn_kot_volgnr
        ;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total := total + lastres;
        iter_cnt := iter_cnt + 1;

        EXIT WHEN lastres = 0 OR iter_cnt >= max_iter;
    END LOOP;

    RETURN total;
END;
$$ LANGUAGE plpgsql;

SELECT kot_derive_g_percelen();