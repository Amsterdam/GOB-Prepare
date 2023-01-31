CREATE TEMPORARY TABLE kot_ontstaan_uit AS
SELECT kot.id         AS kot_id,
       kot.volgnummer AS kot_volgnummer,
       ARRAY_TO_JSON(
               ARRAY_AGG(
                       JSON_BUILD_OBJECT(
                               'kot_identificatie', ontst_uit_kot.identificatie,
                               'kot_id', ontst_uit_kot.id,
                               'kot_volgnummer', ontst_uit_kot.volgnummer
                           ) ORDER BY
                           ontst_uit_kot.identificatie,
                           ontst_uit_kot.id,
                           ontst_uit_kot.volgnummer
                   )
           )          AS ontstaan_uit_kadastraalobject
FROM brk2_prep.kadastraal_object kot
         JOIN brk2_prep.zakelijk_recht zrt_o
              ON zrt_o.__rust_op_kot_id = kot.id
                  AND zrt_o.__rust_op_kot_volgnummer = kot.volgnummer
                  AND zrt_o.ontstaan_uit_appartementsrechtsplitsing_vve IS NOT NULL
         JOIN brk2_prep.zakelijk_recht zrt_b
              ON zrt_o.ontstaan_uit_appartementsrechtsplitsing_vve =
                 zrt_b.betrokken_bij_appartementsrechtsplitsing_vve
                  AND zrt_b.betrokken_bij_appartementsrechtsplitsing_vve IS NOT NULL
         JOIN brk2_prep.kadastraal_object ontst_uit_kot
              ON zrt_b.__rust_op_kot_id = ontst_uit_kot.id AND
                 zrt_b.__rust_op_kot_volgnummer = ontst_uit_kot.volgnummer
WHERE kot.indexletter = 'A'
GROUP BY kot.id, kot.volgnummer;

UPDATE brk2_prep.kadastraal_object kot
SET is_ontstaan_uit_brk_kadastraalobject=t.ontstaan_uit_kadastraalobject
FROM kot_ontstaan_uit t
WHERE kot.is_ontstaan_uit_brk_kadastraalobject IS NULL
  AND kot.indexletter = 'A'
  AND t.kot_id = kot.id
  AND t.kot_volgnummer = kot.volgnummer;
