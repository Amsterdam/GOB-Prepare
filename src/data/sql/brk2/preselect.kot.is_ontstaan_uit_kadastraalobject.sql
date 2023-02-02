SELECT
    kot_id,
    kot_volgnummer,
    jsonb_agg(
            jsonb_build_object(
                    'kot_identificatie', kot_identificatie
                ) ORDER BY kot_identificatie
        )::jsonb AS is_ontstaan_uit_brk_kadastraalobject
FROM (SELECT kot.id         AS kot_id,
             kot.volgnummer AS kot_volgnummer,
             ontst_uit_kot.identificatie AS kot_identificatie
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
      GROUP BY kot.id, kot.volgnummer, ontst_uit_kot.identificatie
     ) q
GROUP BY kot_id, kot_volgnummer;
