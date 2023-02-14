ALTER TABLE brk_prep.kot_ontstaan_uit_g_perceel
    RENAME TO kot_ontstaan_uit_g_perceel_doubles;

CREATE TABLE brk_prep.kot_ontstaan_uit_g_perceel AS
SELECT nrn_kot_id,
       nrn_kot_volgnr,
       JSONB_AGG(
               JSONB_BUILD_OBJECT(
                       'brk_kot_id', brk_kot_id
                   ) ORDER BY brk_kot_id
           ) AS relatie_g_perceel
FROM (SELECT kot.nrn_kot_id,
             kot.nrn_kot_volgnr,
             gperc ->> 'brk_kot_id' AS brk_kot_id
      FROM brk_prep.kot_ontstaan_uit_g_perceel_doubles kot
               LEFT JOIN JSONB_ARRAY_ELEMENTS(kot.relatie_g_perceel) gperc ON TRUE
      WHERE kot.relatie_g_perceel IS NOT NULL
      GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr, gperc ->> 'brk_kot_id') q
GROUP BY nrn_kot_id, nrn_kot_volgnr
;

ANALYZE brk_prep.kot_ontstaan_uit_g_perceel;
