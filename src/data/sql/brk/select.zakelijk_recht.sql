SELECT zrt.id
      ,zrt.identificatie
      ,zrt.aardzakelijkrecht_code
      ,azt.waarde AS aardzakelijkrecht_oms
      ,azt.akr_code AS aardzakelijkrecht_akr_code
      ,blm.is_belast_met AS is_belast_met
      ,bel.belast AS belast
      -- onstaan_uit_ref and betrokken_bij_ref. populate later
     ,zrt_asg.ontstaan_uit_ref
     ,zrt_asg.betrokken_bij_ref
      ,zrt_asg.ontstaan_uit_asg_id
     ,zrt_asg.betrokken_bij_asg_id
      ,zrt_asg.asg_app_rechtsplitstype_code
     ,zrt_asg.asg_app_rechtsplitstype_oms
    ,zrt.isbeperkt_tot
     ,zrt_asg.nrn_asg_id
     ,zrt_asg.asg_einddatum
     ,zrt_asg.asg_actueel
      ,zrt.rust_op_kadastraalobject_id
      ,zrt.rust_op_kadastraalobj_volgnr
      ,zrt_kot.kadastraal_object_id
      ,zrt_kot.zrt_einddatum
      ,zrt_kot.zrt_begindatum
      ,zrt_kot.kot_status_code
      ,zrt_kot.toestandsdatum
      ,zrt_kot.expiration_date
      ,zrt_kot.creation
      ,zrt_kot.modification
      ,betrokken_bij.max_zrt_begindatum AS max_betrokken_bij_begindatum
      ,ontstaan_uit.max_zrt_begindatum AS max_ontstaan_uit_begindatum
FROM   brk.zakelijkrecht zrt
LEFT JOIN (
    SELECT
        zrt_id,
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) as is_belast_met
    FROM (
        SELECT
            zit.zakelijkrecht_id AS zrt_id,
            zrt2.identificatie AS identificatie
        FROM brk.zakelijkrecht_isbelastmet zit
        LEFT JOIN brk.zakelijkrecht zrt2
        ON zrt2.id = zit.is_belast_met
        ) sq
     GROUP BY zrt_id
) blm ON blm.zrt_id=zrt.id
LEFT JOIN (
    SELECT
        zrt_id,
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) as belast
    FROM (
        SELECT
            zit.is_belast_met AS zrt_id,
            zrt2.identificatie AS identificatie
        FROM brk.zakelijkrecht_isbelastmet zit
        LEFT JOIN brk.zakelijkrecht zrt2
        ON zrt2.id = zit.zakelijkrecht_id
    ) sq
    GROUP BY zrt_id
) bel ON bel.zrt_id=zrt.id
LEFT JOIN brk_prep.aardzakelijkrecht_waardelijst azt ON zrt.aardzakelijkrecht_code=azt.code
LEFT JOIN brk_prep.zrt_kot zrt_kot             ON zrt.rust_op_kadastraalobject_id=zrt_kot.rust_op_kadastraalobject_id
                                                    AND zrt.rust_op_kadastraalobj_volgnr=zrt_kot.rust_op_kadastraalobj_volgnr
LEFT JOIN brk_prep.zrt_asg zrt_asg ON zrt.id=zrt_asg.id
                                                    AND zrt.rust_op_kadastraalobj_volgnr=zrt_asg.rust_op_kadastraalobj_volgnr
LEFT JOIN (
    SELECT
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) betrokken_bij,
        max(zrt_begindatum) max_zrt_begindatum,
        ontstaan_uit_asg_id
    FROM (
             SELECT identificatie,
                    zrt2.ontstaan_uit_asg_id,
                    max(zrt2.zrt_begindatum) zrt_begindatum
             FROM brk_prep.zakelijk_recht zrt2
             GROUP BY identificatie, zrt2.ontstaan_uit_asg_id
         ) q
    GROUP BY ontstaan_uit_asg_id
) betrokken_bij ON betrokken_bij.ontstaan_uit_asg_id = zrt_asg.betrokken_bij_asg_id
LEFT JOIN (
    SELECT
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) ontstaan_uit,
        max(zrt_begindatum) max_zrt_begindatum,
        betrokken_bij_asg_id
    FROM (
             SELECT identificatie,
                    zrt2.betrokken_bij_asg_id,
                    max(zrt2.zrt_begindatum) zrt_begindatum
             FROM brk_prep.zakelijk_recht zrt2
             GROUP BY identificatie, zrt2.betrokken_bij_asg_id
         ) q
    GROUP BY betrokken_bij_asg_id
) ontstaan_uit ON ontstaan_uit.betrokken_bij_asg_id = zrt_asg.ontstaan_uit_asg_id
