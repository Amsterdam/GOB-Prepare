  SELECT -- /*+ PARALLEL(4) */ -- tijdelijk? uitgeschakeld?
          sdl.identificatie AS brk_sdl_id
         ,sdl.id AS nrn_sdl_id
         ,sdl.aardstukdeel_code AS sdl_aard_stukdeel_code
         ,asl.omschrijving AS sdl_aard_stukdeel_oms
         ,sdl.bedragtransactiesomlevering AS sdl_koopsom
         ,sdl.bedrtransactiesomlev_vlt_code AS sdl_koopsom_valuta
         ,stk.identificatie AS brk_stk_id
         ,stk.id AS nrn_stk_id
         ,stk.akrportefeuillenr AS stk_akr_portefeuillenr
         ,stk.tijdstip_aanbieding AS stk_tijdstip_aanbieding
         ,stk.reeks_code AS stk_reeks_code
         ,stk.nummer AS stk_volgnummer
         ,stk.registercode_code AS stk_registercode_code
         ,rce.omschrijving AS stk_registercode_oms
         ,stk.soortregister_code AS stk_soortregister_code
         ,srr.omschrijving AS stk_soortregister_oms
         ,stk.deel AS stk_deel_soort
         ,tng.identificatie AS brk_tng_id
         ,tng.id AS nrn_tng_id
         ,NULL AS brk_atg_id
         ,NULL AS nrn_atg_id
         ,NULL AS brk_asg_vve
        -- ,nvl(tng.tng_actueel
        --     ,'FALSE') AS ind_actueel
         ,bsd.brk_bsd_toestandsdatum       AS toestandsdatum
         FROM   brk.stukdeel sdl
         LEFT   JOIN brk.stuk stk
         ON     (sdl.stuk_id = stk.id)
         LEFT   JOIN brk.c_aardstukdeel asl
         ON     (sdl.aardstukdeel_code = asl.code)
         LEFT   JOIN brk.c_soortregister srr
         ON     (stk.registercode_code = srr.code)
         LEFT   JOIN brk.c_registercode rce
         ON     (stk.soortregister_code = rce.code)
         LEFT   JOIN brk.tenaamstelling_isgebaseerdop tip
         ON     (sdl.identificatie = tip.stukdeel_identificatie)
         JOIN   brk.tenaamstelling tng
         ON     (tip.tenaamstelling_id = tng.id)
         JOIN   brk.zakelijkrecht zrt
         ON     tng.van_id=zrt.id
        JOIN    brk.bestand bsd                          ON (1 = 1)
         --
         --
         UNION
         -- selectie vanuit AANTEKENINGISGEBASEERDOP
         --
  SELECT sdl.identificatie AS brk_sdl_id
               ,sdl.id AS nrn_sdl_id
               ,sdl.aardstukdeel_code AS sdl_aard_stukdeel_code
               ,asl.omschrijving AS sdl_aard_stukdeel_oms
               ,sdl.bedragtransactiesomlevering AS sdl_koopsom
               ,sdl.bedrtransactiesomlev_vlt_code AS sdl_koopsom_valuta
               ,stk.identificatie AS brk_stk_id
               ,stk.id AS nrn_stk_id
               ,stk.akrportefeuillenr AS stk_akr_portefeuillenr
               ,stk.tijdstip_aanbieding AS stk_tijdstip_aanbieding
               ,stk.reeks_code AS stk_reeks_code
               ,stk.nummer AS stk_volgnummer
               ,stk.registercode_code AS stk_registercode_code
               ,rce.omschrijving AS stk_registercode_oms
               ,stk.soortregister_code AS stk_soortregister_code
               ,srr.omschrijving AS stk_soortregister_oms
               ,stk.deel AS stk_deel_soort
               ,NULL AS brk_tng_id
               ,NULL AS nrn_tng_id
               ,atg.identificatie AS brk_atg_id
               ,atg.id AS nrn_atg_id
               ,NULL AS brk_asg_vve
           --    ,nvl(atg.ind_actueel
           --        ,'FALSE') AS ind_actueel
               ,bsd.brk_bsd_toestandsdatum       AS toestandsdatum
         FROM   brk.stukdeel sdl
         JOIN   brk.stuk stk
         ON     (sdl.stuk_id = stk.id)
         LEFT   JOIN brk.c_aardstukdeel asl
         ON     (sdl.aardstukdeel_code = asl.code)
         LEFT   JOIN brk.c_soortregister srr
         ON     (stk.registercode_code = srr.code)
         LEFT   JOIN brk.c_registercode rce
         ON     (stk.soortregister_code = rce.code)
         LEFT   JOIN brk.aantekeningisgebaseerdop aip
         ON     (sdl.identificatie = aip.stukdeel_identificatie)
         LEFT   JOIN brk.aantekening atg
         ON     (aip.aantekening_id = atg.id)
         JOIN   brk.bestand bsd                          ON (1 = 1)
         --
         -- selectie vanuit APPARTEMENTSRECHTSPL_STUKDEEL
         UNION
         --
         SELECT sdl.identificatie AS brk_sdl_id
               ,sdl.id AS nrn_sdl_id
               ,sdl.aardstukdeel_code AS sdl_aard_stukdeel_code
               ,asl.omschrijving AS sdl_aard_stukdeel_oms
               ,sdl.bedragtransactiesomlevering AS sdl_koopsom
               ,sdl.bedrtransactiesomlev_vlt_code AS sdl_koopsom_valuta
               ,stk.identificatie AS brk_stk_id
               ,stk.id AS nrn_stk_id
               ,stk.akrportefeuillenr AS stk_akr_portefeuillenr
               ,stk.tijdstip_aanbieding AS stk_tijdstip_aanbieding
               ,stk.reeks_code AS stk_reeks_code
               ,stk.nummer AS stk_volgnummer
               ,stk.registercode_code AS stk_registercode_code
               ,rce.omschrijving AS stk_registercode_oms
               ,stk.soortregister_code AS stk_soortregister_code
               ,srr.omschrijving AS stk_soortregister_oms
               ,stk.deel AS stk_deel_soort
               ,NULL AS brk_tng_id
               ,NULL AS nrn_tng_id
               ,NULL AS brk_atg_id
               ,NULL AS nrn_atg_id
               ,asg.vve AS brk_asg_vve
         --      ,'TRUE' AS ind_actueel
               ,bsd.brk_bsd_toestandsdatum       AS toestandsdatum
         FROM   brk.stukdeel sdl
         JOIN   brk.stuk stk
         ON     (sdl.stuk_id = stk.id)
         LEFT   JOIN brk.c_aardstukdeel asl
         ON     (sdl.aardstukdeel_code = asl.code)
         LEFT   JOIN brk.c_soortregister srr
         ON     (stk.registercode_code = srr.code)
         LEFT   JOIN brk.c_registercode rce
         ON     (stk.soortregister_code = rce.code)
         LEFT   JOIN brk.appartementsrechtspl_stukdeel arl
         ON     (sdl.identificatie = arl.stukdeel_identificatie)
         LEFT   JOIN brk.appartementsrechtsplitsing asg
         ON     (arl.appartementsrechtsplitsing_id = asg.id)
         JOIN   brk.bestand bsd                          ON (1 = 1)
