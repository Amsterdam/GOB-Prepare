-- Expects ZRT and TNG to be prepared
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
     , art.art_ids						 AS art_ids
     , akt.akt_ids                       AS akt_ids
     , asg.zrt_ids 						 AS zrt_ids
     , least(tng.begindatum, asg.zrt_begindatum) AS begindatum
     , CASE
         -- If any einddatum is NULL this stukdeel is still valid. Otherwise get the max
        WHEN tng.einddatum IS NULL OR asg.zrt_begindatum IS NULL
        THEN NULL
        ELSE greatest(tng.einddatum, asg.zrt_begindatum)
       END                              AS einddatum
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
                min(tng.begindatum) AS begindatum,
                CASE
                    WHEN sum(CASE WHEN tng.einddatum IS NULL THEN 0 ELSE 1 END) < SUM(1)
                    THEN NULL
                    ELSE max(tng.einddatum)
                END AS einddatum,
                array_to_json(array_agg(json_build_object(
                    'brk_tng_id', tng.brk_tng_id,
                    'nrn_tng_id', tng.nrn_tng_id
                ))) AS tng_ids
            FROM brk.tenaamstelling_isgebaseerdop tip
            LEFT JOIN brk_prep.tenaamstelling tng
            ON tng.nrn_tng_id=tip.tenaamstelling_id
            GROUP BY tip.stukdeel_identificatie
         ) tng ON (sdl.identificatie=tng.stukdeel_identificatie)
          LEFT JOIN (
            SELECT
                aip.stukdeel_identificatie,
                array_to_json(array_agg(json_build_object(
                    'brk_art_id', atg.identificatie,
                    'nrn_art_id', atg.id
                ))) AS art_ids
            FROM brk.aantekeningisgebaseerdop aip
            LEFT JOIN brk.aantekening atg
            ON atg.id=aip.aantekening_id
            JOIN brk.aantekeningrecht art -- Only include ART's
            ON art.aantekening_id=atg.id
            GROUP BY aip.stukdeel_identificatie
         ) art ON (sdl.identificatie=art.stukdeel_identificatie)
         LEFT JOIN (
                SELECT
                    aip.stukdeel_identificatie,
                    array_to_json(array_agg(json_build_object(
                            'brk_akt_id', atg.identificatie,
                            'nrn_akt_id', atg.id
                        ))) AS akt_ids
                FROM brk.aantekeningisgebaseerdop aip
                 LEFT JOIN brk.aantekening atg
                           ON atg.id=aip.aantekening_id
                 JOIN brk.aantekening_kadastraalobject akt -- Only include AKT's
                      ON akt.aantekening_id=atg.id
                GROUP BY aip.stukdeel_identificatie
            ) akt ON (sdl.identificatie=akt.stukdeel_identificatie)
         LEFT JOIN (
            SELECT
                stukdeel_identificatie,
                min(zrt_begindatum) AS zrt_begindatum,
                CASE
                    -- NULL if one of the zrt_einddatums is NULL, otherwise max
                        WHEN SUM(CASE WHEN zrt_einddatum IS NULL THEN 0 ELSE 1 END ) < SUM(1)
                        THEN NULL
                        ELSE max(zrt_einddatum)
                    END AS zrt_einddatum,
                array_to_json(array_agg(json_build_object(
                    'brk_zrt_id', identificatie
                ))) AS zrt_ids
             FROM (
                SELECT
                    arl.stukdeel_identificatie,
                    min(zrt.zrt_begindatum) AS zrt_begindatum,
                    CASE
                        -- NULL if one of the zrt_einddatums is NULL, otherwise max
                        WHEN SUM(CASE WHEN zrt.zrt_einddatum IS NULL THEN 0 ELSE 1 END ) < SUM(1)
                        THEN NULL
                        ELSE max(zrt.zrt_einddatum)
                    END AS zrt_einddatum,
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