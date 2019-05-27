-- Query adds 'onderliggende_objecten' and 'relatie_g_perceel' relations to A percelen in the KOT table.
--
-- 'onderliggende_objecten' is the first order relation through zakelijk_recht. These objects could be 'A' or 'G'
-- percelen.
-- 'relatie_g_perceel' is the result of resolving the 'onderliggende_objecten' relation until a relation with 'G'
-- percelen is found; this implies that 'onderliggende_objecten' and 'relatie_g_perceel' could have the same value.

-- Create helpful indexes
CREATE INDEX ON brk_prep.zakelijk_recht (rust_op_kadastraalobject_id, rust_op_kadastraalobj_volgnr);
CREATE INDEX ON brk_prep.zakelijk_recht (ontstaan_uit);
CREATE INDEX ON brk_prep.zakelijk_recht (identificatie);
CREATE INDEX ON brk_prep.kadastraal_object (index_letter);
CREATE INDEX ON brk_prep.kadastraal_object (brk_kot_id);
CREATE INDEX ON brk_prep.kadastraal_object (nrn_kot_volgnr);
CREATE INDEX ON brk_prep.kadastraal_object (nrn_kot_id, nrn_kot_volgnr);
CREATE INDEX ON brk_prep.kadastraal_object (brk_kot_id, nrn_kot_volgnr);
CREATE INDEX ON brk_prep.kadastraal_object USING gin (relatie_g_perceel);
CREATE INDEX ON brk_prep.kadastraal_object USING gin (ontstaan_uit_kadastraalobject);

-- Add first order relation
UPDATE brk_prep.kadastraal_object kot
SET ontstaan_uit_kadastraalobject=rel.ontstaan_uit_kadastraalobject
FROM (
         SELECT kot.nrn_kot_id     AS nrn_kot_id,
                kot.nrn_kot_volgnr AS nrn_kot_volgnr,
                array_to_json(array_agg(json_build_object(
                        'brk_kot_id', kot2.brk_kot_id,
                        'nrn_kot_id', kot2.nrn_kot_id,
                        'kot_volgnummer', kot2.nrn_kot_volgnr
                    )))            AS ontstaan_uit_kadastraalobject
         FROM brk_prep.kadastraal_object kot
                  LEFT JOIN brk_prep.zakelijk_recht zrt
                            ON zrt.rust_op_kadastraalobject_id = kot.nrn_kot_id AND
                               zrt.rust_op_kadastraalobj_volgnr = kot.nrn_kot_volgnr
                  JOIN LATERAL jsonb_array_elements(zrt.ontstaan_uit) ontstaan_uit(item) ON TRUE
                  LEFT JOIN brk_prep.zakelijk_recht zrt2
                            ON zrt2.identificatie = ontstaan_uit.item ->> 'zrt_identificatie'
                  LEFT JOIN brk_prep.kadastraal_object kot2
                            ON kot2.nrn_kot_id = zrt2.rust_op_kadastraalobject_id AND
                               kot2.nrn_kot_volgnr = zrt2.rust_op_kadastraalobj_volgnr
         WHERE zrt.ontstaan_uit IS NOT NULL
         GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr
     ) AS rel(nrn_kot_id, nrn_kot_volgnr, ontstaan_uit_kadastraalobject)
WHERE kot.ontstaan_uit_kadastraalobject = 'null'
  AND kot.index_letter = 'A'
  AND rel.nrn_kot_id = kot.nrn_kot_id
  AND rel.nrn_kot_volgnr = kot.nrn_kot_volgnr;

CREATE OR REPLACE FUNCTION add_g_percelen_to_kot() RETURNS integer AS
$$
DECLARE
    total    integer := 0;
    lastres  integer := 0;
    max_iter integer := 3000;
    iter_cnt integer := 0;
BEGIN
    -- Update 'A' percelen directly related to a 'G' perceel
    UPDATE brk_prep.kadastraal_object updatekot
    SET relatie_g_perceel=rel.relatie_g_perceel
    FROM (
             SELECT kot.brk_kot_id,
                    kot.nrn_kot_volgnr,
                    array_to_json(array_agg(json_build_object(
                            'brk_kot_id', kot2.brk_kot_id,
                            'nrn_kot_id', kot2.nrn_kot_id,
                            'kot_volgnummer', kot2.nrn_kot_volgnr
                        ))) AS relatie_g_perceel
             FROM brk_prep.kadastraal_object kot
                      JOIN LATERAL jsonb_array_elements(kot.ontstaan_uit_kadastraalobject) ontstaan_uit_kadastraalobject(item)
                           ON TRUE
                      LEFT JOIN brk_prep.kadastraal_object kot2
                                ON kot2.brk_kot_id = ontstaan_uit_kadastraalobject.item ->> 'brk_kot_id'
             WHERE kot2.index_letter = 'G'
             AND kot.ontstaan_uit_kadastraalobject <> 'null'
             GROUP BY kot.brk_kot_id, kot.nrn_kot_volgnr
         ) AS rel(brk_kot_id, kot_volgnr, relatie_g_perceel)
    WHERE updatekot.relatie_g_perceel = 'null'
      AND updatekot.index_letter = 'A'
      AND rel.brk_kot_id = updatekot.brk_kot_id
      AND rel.kot_volgnr = updatekot.nrn_kot_volgnr;

    GET DIAGNOSTICS lastres = ROW_COUNT;
    total := total + lastres;


    -- Update 'A' percelen with relation to 'A' percelen
    LOOP
        UPDATE brk_prep.kadastraal_object updatekot
        SET relatie_g_perceel=rel.relatie_g_perceel
        FROM (
                 SELECT kot.brk_kot_id,
                        kot.nrn_kot_volgnr,
                        array_to_json(array_agg(json_build_object(
                                'brk_kot_id', kot3.brk_kot_id,
                                'nrn_kot_id', kot3.nrn_kot_id,
                                'kot_volgnummer', kot3.nrn_kot_volgnr
                            ))) AS relatie_g_perceel
                 FROM (
                          SELECT kot.brk_kot_id,
                                 kot.nrn_kot_volgnr,
                                 kot.relatie_g_perceel,
                                 kot.ontstaan_uit_kadastraalobject
                          FROM brk_prep.kadastraal_object kot
                          WHERE (kot.brk_kot_id, kot.nrn_kot_volgnr) IN (
                              SELECT kot.brk_kot_id,
                                     kot.nrn_kot_volgnr
                              FROM brk_prep.kadastraal_object kot
                                       JOIN LATERAL jsonb_array_elements(kot.ontstaan_uit_kadastraalobject) ontstaan_uit_kadastraalobject(item)
                                            ON TRUE
                                       LEFT JOIN brk_prep.kadastraal_object kot2
                                                 ON kot2.brk_kot_id = ontstaan_uit_kadastraalobject.item ->> 'brk_kot_id'
                              WHERE kot2.index_letter = 'A'
                                AND kot2.relatie_g_perceel <> 'null'
                                AND kot.relatie_g_perceel = 'null'
                                AND kot.ontstaan_uit_kadastraalobject <> 'null'
                              GROUP BY kot.brk_kot_id, kot.nrn_kot_volgnr
                              LIMIT 500
                          )
                      ) kot
                          JOIN LATERAL jsonb_array_elements(kot.ontstaan_uit_kadastraalobject) ontstaan_uit_kadastraalobject(item)
                               ON TRUE
                          LEFT JOIN brk_prep.kadastraal_object kot2
                                    ON kot2.brk_kot_id = ontstaan_uit_kadastraalobject.item ->> 'brk_kot_id'
                          JOIN LATERAL jsonb_array_elements(kot2.relatie_g_perceel) rel_g_perceel(item) ON TRUE
                          LEFT JOIN brk_prep.kadastraal_object kot3
                                    ON kot3.brk_kot_id = rel_g_perceel.item ->> 'brk_kot_id'
                 WHERE kot2.index_letter = 'A'
                   AND kot2.relatie_g_perceel <> 'null'
                 GROUP BY kot.brk_kot_id, kot.nrn_kot_volgnr
             ) AS rel(brk_kot_id, nrn_kot_volgnr, relatie_g_perceel)
        WHERE rel.brk_kot_id = updatekot.brk_kot_id
          AND rel.nrn_kot_volgnr = updatekot.nrn_kot_volgnr;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total := total + lastres;
        iter_cnt := iter_cnt + 1;

        EXIT WHEN lastres = 0 OR iter_cnt >= max_iter;
    END LOOP;

    RETURN total;
END;
$$ LANGUAGE plpgsql;

SELECT add_g_percelen_to_kot();