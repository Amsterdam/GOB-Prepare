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
           ) AS ontstaan_uit_kadastraalobject
FROM brk_prep.kadastraal_object kot
         LEFT JOIN brk_prep.zakelijk_recht zrt_o
                   ON zrt_o.rust_op_kadastraalobject_id = kot.nrn_kot_id
                       AND zrt_o.rust_op_kadastraalobj_volgnr = kot.nrn_kot_volgnr
         LEFT JOIN brk_prep.zakelijk_recht zrt_b
                   ON zrt_b.betrokken_bij_asg_id = zrt_o.ontstaan_uit_asg_id
         LEFT JOIN brk_prep.kadastraal_object ontst_uit_kot
                   ON zrt_b.rust_op_kadastraalobject_id = ontst_uit_kot.nrn_kot_id
                       AND zrt_b.rust_op_kadastraalobj_volgnr = ontst_uit_kot.nrn_kot_volgnr
WHERE kot.index_letter = 'A'
  AND zrt_o.ontstaan_uit_asg_id IS NOT NULL
GROUP BY kot.nrn_kot_id, kot.nrn_kot_volgnr;
;
