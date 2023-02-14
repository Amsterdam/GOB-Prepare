SELECT zrt.*,
       betrokken_bij.betrokken_bij,
       betrokken_bij.max_zrt_begindatum AS max_betrokken_bij_begindatum,
       ontstaan_uit.ontstaan_uit,
       ontstaan_uit.max_zrt_begindatum AS max_ontstaan_uit_begindatum
FROM brk_prep.zakelijk_recht_1 zrt
         LEFT JOIN (SELECT ARRAY_TO_JSON(ARRAY_AGG(JSON_BUILD_OBJECT('zrt_identificatie', identificatie)
                                                   ORDER BY identificatie)) betrokken_bij,
                           MAX(zrt_begindatum)                              max_zrt_begindatum,
                           ontstaan_uit_asg_id
                    FROM (SELECT identificatie,
                                 zrt2.ontstaan_uit_asg_id,
                                 MAX(zrt2.zrt_begindatum) zrt_begindatum
                          FROM brk_prep.zakelijk_recht_1 zrt2
                          GROUP BY identificatie, zrt2.ontstaan_uit_asg_id) q
                    GROUP BY ontstaan_uit_asg_id) betrokken_bij
                   ON betrokken_bij.ontstaan_uit_asg_id = zrt.betrokken_bij_asg_id
         LEFT JOIN (SELECT ARRAY_TO_JSON(ARRAY_AGG(JSON_BUILD_OBJECT('zrt_identificatie', identificatie)
                                                   ORDER BY identificatie)) ontstaan_uit,
                           MAX(zrt_begindatum)                              max_zrt_begindatum,
                           betrokken_bij_asg_id
                    FROM (SELECT identificatie,
                                 zrt2.betrokken_bij_asg_id,
                                 MAX(zrt2.zrt_begindatum) zrt_begindatum
                          FROM brk_prep.zakelijk_recht_1 zrt2
                          GROUP BY identificatie, zrt2.betrokken_bij_asg_id) q
                    GROUP BY betrokken_bij_asg_id) ontstaan_uit
                   ON ontstaan_uit.betrokken_bij_asg_id = zrt.ontstaan_uit_asg_id
