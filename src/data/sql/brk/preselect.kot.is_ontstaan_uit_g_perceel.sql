CREATE TABLE brk_prep.kot_ontstaan_uit_g_perceel AS
SELECT kot.nrn_kot_id,
       kot.nrn_kot_volgnr,
       JSONB_AGG(
               JSONB_BUILD_OBJECT(
                       'brk_kot_id', ontst_uit_kot.brk_kot_id,
                       'nrn_kot_id', ontst_uit_kot.nrn_kot_id,
                       'kot_volgnummer', ontst_uit_kot.nrn_kot_volgnr
                   ) ORDER BY
                   ontst_uit_kot.brk_kot_id,
                   ontst_uit_kot.nrn_kot_id,
                   ontst_uit_kot.nrn_kot_volgnr
           ) AS relatie_g_perceel
FROM brk_prep.kot_ontstaan_uit_kot kot
         LEFT JOIN JSONB_ARRAY_ELEMENTS(kot.ontstaan_uit_kadastraalobject) json_elms(obj)
                   ON TRUE
         LEFT JOIN brk_prep.kadastraal_object ontst_uit_kot
                   ON ontst_uit_kot.nrn_kot_id = (json_elms.obj ->> 'nrn_kot_id')::integer
                       AND ontst_uit_kot.nrn_kot_volgnr = (json_elms.obj ->> 'kot_volgnummer')::integer
WHERE ontst_uit_kot.index_letter = 'G'
  AND kot.ontstaan_uit_kadastraalobject IS NOT NULL
GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr
;

ANALYZE brk_prep.kot_ontstaan_uit_g_perceel;
