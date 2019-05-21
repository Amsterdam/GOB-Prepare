SELECT
    sdl.identificatie                    AS brk_sdl_id
     , sdl.id                            AS nrn_sdl_id
     , sdl.aardstukdeel_code             AS sdl_aard_stukdeel_code
     , asl.omschrijving                  AS sdl_aard_stukdeel_oms
     , sdl.bedragtransactiesomlevering   AS sdl_koopsom
     , sdl.bedrtransactiesomlev_vlt_code AS sdl_koopsom_valuta
     , stk.identificatie                 AS brk_stk_id
     , stk.id                            AS nrn_stk_id
     , stk.akrportefeuillenr             AS stk_akr_portefeuillenr
     , stk.tijdstip_aanbieding           AS stk_tijdstip_aanbieding
     , stk.reeks_code                    AS stk_reeks_code
     , stk.nummer                        AS stk_volgnummer
     , stk.registercode_code             AS stk_registercode_code
     , rce.omschrijving                  AS stk_registercode_oms
     , stk.soortregister_code            AS stk_soortregister_code
     , srr.omschrijving                  AS stk_soortregister_oms
     , stk.deel                          AS stk_deel_soort
     , tng.tng_ids						 AS tng_ids
     , atg.atg_ids						 AS atg_ids
     , asg.zrt_ids 						 AS zrt_ids
     , bsd.brk_bsd_toestandsdatum        AS toestandsdatum
FROM brk.stukdeel sdl
         LEFT JOIN brk.stuk stk
                   ON (sdl.stuk_id = stk.id)
         LEFT JOIN brk.c_aardstukdeel asl
                   ON (sdl.aardstukdeel_code = asl.code)
         LEFT JOIN brk.c_soortregister srr
                   ON (stk.registercode_code = srr.code)
         LEFT JOIN brk.c_registercode rce
                   ON (stk.soortregister_code = rce.code)
         LEFT JOIN (
            SELECT
                tip.stukdeel_identificatie,
                array_to_json(array_agg(json_build_object(
                    'brk_tng_id', tng.identificatie,
                    'nrn_tng_id', tng.id
                ))) AS tng_ids
            FROM brk.tenaamstelling_isgebaseerdop tip
            LEFT JOIN brk.tenaamstelling tng
            ON tng.id=tip.tenaamstelling_id
            GROUP BY tip.stukdeel_identificatie
         ) tng ON (sdl.identificatie=tng.stukdeel_identificatie)
          LEFT JOIN (
            SELECT
                aip.stukdeel_identificatie,
                array_to_json(array_agg(json_build_object(
                    'brk_atg_id', atg.identificatie,
                    'nrn_atg_id', atg.id
                ))) AS atg_ids
            FROM brk.aantekeningisgebaseerdop aip
            LEFT JOIN brk.aantekening atg
            ON atg.id=aip.aantekening_id
            GROUP BY aip.stukdeel_identificatie
         ) atg ON (sdl.identificatie=atg.stukdeel_identificatie)
         LEFT JOIN (
            SELECT
                stukdeel_identificatie,
                array_to_json(array_agg(json_build_object(
                    'brk_zrt_id', identificatie
                ))) AS zrt_ids
             FROM (
                SELECT
                    arl.stukdeel_identificatie,
                    zrt.identificatie
                FROM brk.appartementsrechtspl_stukdeel arl
                LEFT JOIN brk.appartementsrechtsplitsing asg
                ON (arl.appartementsrechtsplitsing_id = asg.id)
                LEFT JOIN brk_prep.zakelijk_recht zrt
                ON (zrt.ontstaan_uit_asg_id=asg.id)
                GROUP BY arl.stukdeel_identificatie, zrt.identificatie
             ) q
            GROUP BY stukdeel_identificatie
         ) asg ON (asg.stukdeel_identificatie=sdl.identificatie)
         JOIN brk.bestand bsd ON (1 = 1)