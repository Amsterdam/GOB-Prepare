ALTER TABLE brk_prep.kot_ontstaan_uit_kot
    RENAME TO kot_ontstaan_uit_kot_doubles;

CREATE TABLE brk_prep.kot_ontstaan_uit_kot AS
SELECT nrn_kot_id,
       nrn_kot_volgnr,
       JSONB_AGG(
               JSONB_BUILD_OBJECT(
                       'brk_kot_id', brk_kot_id
                   ) ORDER BY brk_kot_id
           ) AS ontstaan_uit_kadastraalobject
FROM (SELECT kot.nrn_kot_id,
             kot.nrn_kot_volgnr,
             gperc ->> 'brk_kot_id' AS brk_kot_id
      FROM brk_prep.kot_ontstaan_uit_kot_doubles kot
               LEFT JOIN JSONB_ARRAY_ELEMENTS(kot.ontstaan_uit_kadastraalobject) gperc ON TRUE
      WHERE kot.ontstaan_uit_kadastraalobject IS NOT NULL
      GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr, gperc ->> 'brk_kot_id') q
GROUP BY nrn_kot_id, nrn_kot_volgnr
;

ANALYZE brk_prep.kot_ontstaan_uit_kot;
