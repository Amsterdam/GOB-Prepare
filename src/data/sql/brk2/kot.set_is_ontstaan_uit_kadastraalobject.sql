UPDATE brk2_prep.kadastraal_object kot
SET is_ontstaan_uit_kadastraalobject=q.ontstaan_uit_kadastraalobject
FROM (SELECT kot.id,
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
               JOIN brk2.zakelijkrecht zrt_o
                    ON zrt_o.rust_op_kadastraalobject_id = kot.id
                        AND zrt_o.rust_op_kadastraalobj_volgnr = kot.volgnummer
                        AND zrt_o.isontstaanuit_identificatie IS NOT NULL
               JOIN brk2.zakelijkrecht zrt_b
                    ON zrt_o.isontstaanuit_identificatie = zrt_b.isbetrokkenbij_identificatie
                        AND zrt_b.isbetrokkenbij_identificatie IS NOT NULL
               JOIN brk2.kadastraal_object ontst_uit_kot
                    ON zrt_b.rust_op_kadastraalobject_id = ontst_uit_kot.id AND
                       zrt_b.rust_op_kadastraalobj_volgnr = ontst_uit_kot.volgnummer
      WHERE kot.indexletter = 'A'
      GROUP BY kot.id, kot.volgnummer) q(kot_id, kot_volgnummer, ontstaan_uit_kadastraalobject)
WHERE kot.is_ontstaan_uit_kadastraalobject is null
  AND kot.indexletter = 'A'
  AND q.kot_id = kot.id
  AND q.kot_volgnummer = kot.volgnummer
