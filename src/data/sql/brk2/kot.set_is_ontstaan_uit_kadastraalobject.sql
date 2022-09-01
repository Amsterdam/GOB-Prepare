UPDATE brk2_prep.kadastraal_object kot
SET is_ontstaan_uit_kadastraalobject=q.ontstaan_uit_kadastraalobject
FROM (
         SELECT
             kot.id,
             kot.volgnummer,
             array_to_json(
                     array_agg(
                             json_build_object(
                                     'kot_identificatie', ontst_uit_kot.identificatie,
                                     'kot_id', ontst_uit_kot.id,
                                     'kot_volgnummer', ontst_uit_kot.volgnummer
                                 ) ORDER BY
                                 ontst_uit_kot.identificatie,
                                 ontst_uit_kot.id,
                                 ontst_uit_kot.volgnummer
                         )
                 ) AS ontstaan_uit_kadastraalobject
         FROM brk2_prep.kadastraal_object kot
                  LEFT JOIN brk2_prep.zakelijk_recht zrt_o
                            ON zrt_o.__rust_op_kot_id=kot.id
                                AND zrt_o.__rust_op_kot_volgnummer=kot.volgnummer
                  LEFT JOIN brk2_prep.zakelijk_recht zrt_b
                            ON zrt_b.__betrokken_bij_asg_id=zrt_o.__ontstaan_uit_asg_id
                  LEFT JOIN brk2_prep.kadastraal_object ontst_uit_kot
                            ON zrt_b.__rust_op_kot_id=ontst_uit_kot.id
                                AND zrt_b.__rust_op_kot_volgnummer=ontst_uit_kot.volgnummer
         WHERE kot.indexletter = 'A'
           AND zrt_o.__ontstaan_uit_asg_id IS NOT NULL
         GROUP BY kot.id, kot.volgnummer
     ) q(kot_id, kot_volgnummer, ontstaan_uit_kadastraalobject)
WHERE kot.is_ontstaan_uit_kadastraalobject is null
  AND kot.indexletter = 'A'
  AND q.kot_id = kot.id
  AND q.kot_volgnummer = kot.volgnummer
;