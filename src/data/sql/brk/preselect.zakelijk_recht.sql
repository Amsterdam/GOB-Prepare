SELECT zrt.id
     , zrt.identificatie
     , zrt.aardzakelijkrecht_code
     , azt.waarde                       AS aardzakelijkrecht_oms
     , azt.akrcode                      AS aardzakelijkrecht_akr_code
     , blm.is_belast_met                AS is_belast_met
     , bel.belast                       AS belast
     , zrt_asg.ontstaan_uit_ref
     , zrt_asg.betrokken_bij_ref
     , zrt_asg.ontstaan_uit_asg_id
     , zrt_asg.betrokken_bij_asg_id
     , zrt_asg.asg_app_rechtsplitstype_code
     , zrt_asg.asg_app_rechtsplitstype_oms
     , zrt.isbeperkt_tot
     , zrt_asg.nrn_asg_id
     , zrt_asg.asg_einddatum
     , zrt_asg.asg_actueel
     , zrt_kot.rust_op_kadastraalobject_id
     , zrt_kot.rust_op_kadastraalobj_volgnr
     , zrt_kot.kadastraal_object_id
     , zrt_kot.zrt_einddatum
     , zrt_kot.zrt_begindatum
     , zrt_kot.kot_status_code
     , zrt_kot.toestandsdatum
     , zrt_kot.expiration_date
     , zrt_kot.creation
     , zrt_kot.modification
FROM brk.zakelijkrecht zrt
         LEFT JOIN (SELECT zrt_id,
                           ARRAY_TO_JSON(ARRAY_AGG(JSON_BUILD_OBJECT('zrt_identificatie', identificatie)
                                                   ORDER BY identificatie)) AS is_belast_met
                    FROM (SELECT zit.zakelijkrecht_id AS zrt_id,
                                 zrt2.identificatie   AS identificatie
                          FROM brk.zakelijkrecht_isbelastmet zit
                                   LEFT JOIN brk.zakelijkrecht zrt2
                                             ON zrt2.id = zit.is_belast_met) sq
                    GROUP BY zrt_id) blm ON blm.zrt_id = zrt.id
         LEFT JOIN (SELECT zrt_id,
                           ARRAY_TO_JSON(ARRAY_AGG(JSON_BUILD_OBJECT('zrt_identificatie', identificatie)
                                                   ORDER BY identificatie)) AS belast
                    FROM (SELECT zit.is_belast_met  AS zrt_id,
                                 zrt2.identificatie AS identificatie
                          FROM brk.zakelijkrecht_isbelastmet zit
                                   LEFT JOIN brk.zakelijkrecht zrt2
                                             ON zrt2.id = zit.zakelijkrecht_id) sq
                    GROUP BY zrt_id) bel ON bel.zrt_id = zrt.id
         LEFT JOIN brk_prep.aardzakelijkrecht_waardelijst azt ON zrt.aardzakelijkrecht_code = azt.code
         LEFT JOIN brk_prep.zrt_kot zrt_kot ON zrt.id = zrt_kot.id
         LEFT JOIN brk_prep.zrt_asg zrt_asg ON zrt.id = zrt_asg.id
;
