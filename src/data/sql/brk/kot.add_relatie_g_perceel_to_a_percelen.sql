-- Adds 'relatie_g_perceel' values to A percelen in the kot_ontstaan_uit_g_perceel
--
-- 'ontstaan_uit_kadastraalobject' contains the direct parent(s) of an A-perceel.
-- 'relatie_g_perceel' contains the G-perceel predecessors of an A-perceel.

-- Add first order relation. Set ontstaan_uit_kadastraalobject for all A-percelen
-- Relation is derived through the ontstaan_uit_asg_id and betrokken_bij_asg_id values from 'zakelijk_recht'.
-- ASG stands for 'appartementsrechtsplitsing'. When A has the same value for 'betrokken_bij' as B for 'onstaan_uit',
-- they were involved in the same ASG. In this case A is a parent for B (there can be multiple parents).
--
-- In this query we set all direct parent <-> child relation

-- Set relatie_g_perceel for all other A-percelen.
-- Inherit all items in 'relatie_g_perceel' of parent objects.
CREATE OR REPLACE FUNCTION kot_derive_g_percelen() RETURNS integer AS
$$
DECLARE
    total    integer := 0;
    lastres  integer := 0;
    max_iter integer := 20;
    iter_cnt integer := 0;
BEGIN
    CREATE TEMPORARY TABLE kot_g_percelen AS
    SELECT kot.nrn_kot_id,
           kot.brk_kot_id,
           kot.nrn_kot_volgnr,
           ontst_gperc.relatie_g_perceel,
           ontst_kot.ontstaan_uit_kadastraalobject
    FROM brk_prep.kadastraal_object kot
             LEFT JOIN brk_prep.kot_ontstaan_uit_g_perceel ontst_gperc USING (nrn_kot_id, nrn_kot_volgnr)
             LEFT JOIN brk_prep.kot_ontstaan_uit_kot ontst_kot USING (nrn_kot_id, nrn_kot_volgnr)
    WHERE kot.index_letter = 'A';

    CREATE INDEX ON kot_g_percelen (nrn_kot_id, nrn_kot_volgnr);
    CREATE INDEX ON kot_g_percelen USING GIN (relatie_g_perceel);
    CREATE INDEX ON kot_g_percelen USING GIN (ontstaan_uit_kadastraalobject);

    ANALYZE kot_g_percelen;

    LOOP
        UPDATE kot_g_percelen kot
        SET relatie_g_perceel=q.relatie_g_perceel
        FROM (SELECT kot.nrn_kot_id,
                     kot.nrn_kot_volgnr,
                     ARRAY_TO_JSON(
                             ARRAY_AGG(
                                     JSON_BUILD_OBJECT(
                                             'brk_kot_id', gperceel.brk_kot_id,
                                             'nrn_kot_id', gperceel.nrn_kot_id,
                                             'kot_volgnummer', gperceel.nrn_kot_volgnr
                                         ) ORDER BY
                                         gperceel.brk_kot_id,
                                         gperceel.nrn_kot_id,
                                         gperceel.nrn_kot_volgnr
                                 )
                         ) AS relatie_g_perceel
              FROM kot_g_percelen kot
                       LEFT JOIN JSONB_ARRAY_ELEMENTS(kot.ontstaan_uit_kadastraalobject) json_elms(obj)
                                 ON TRUE
                       LEFT JOIN kot_g_percelen ontst_uit_kot
                                 ON ontst_uit_kot.nrn_kot_id = (json_elms.obj ->> 'nrn_kot_id')::integer
                                     AND ontst_uit_kot.nrn_kot_volgnr = (json_elms.obj ->> 'kot_volgnummer')::integer
                       LEFT JOIN JSONB_ARRAY_ELEMENTS(ontst_uit_kot.relatie_g_perceel) json_gperc_elms(obj)
                                 ON TRUE
                       LEFT JOIN kot_g_percelen gperceel
                                 ON gperceel.nrn_kot_id = (json_gperc_elms.obj ->> 'nrn_kot_id')::integer
                                     AND gperceel.nrn_kot_volgnr = (json_gperc_elms.obj ->> 'kot_volgnummer')::integer
              WHERE ontst_uit_kot.relatie_g_perceel IS NOT NULL
                AND kot.relatie_g_perceel IS NULL
                AND kot.ontstaan_uit_kadastraalobject IS NOT NULL
              GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr) q(nrn_kot_id, nrn_kot_volgnr, relatie_g_perceel)
        WHERE kot.nrn_kot_id = q.nrn_kot_id
          AND kot.nrn_kot_volgnr = q.nrn_kot_volgnr;
        ANALYZE kot_g_percelen;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total := total + lastres;
        iter_cnt := iter_cnt + 1;

        EXIT WHEN lastres = 0 OR iter_cnt >= max_iter;
    END LOOP;

    -- Replace old table with results
    DROP TABLE brk_prep.kot_ontstaan_uit_g_perceel;
    CREATE TABLE brk_prep.kot_ontstaan_uit_g_perceel AS
    SELECT nrn_kot_id, nrn_kot_volgnr, relatie_g_perceel FROM kot_g_percelen WHERE relatie_g_perceel IS NOT NULL;
    ANALYZE brk_prep.kot_ontstaan_uit_g_perceel;

    RETURN total;
END ;
$$ LANGUAGE plpgsql;

SELECT kot_derive_g_percelen();
