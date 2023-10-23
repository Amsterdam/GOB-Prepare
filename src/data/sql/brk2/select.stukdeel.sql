SELECT sdl.id                                                               AS id,
       sdl.identificatie                                                    AS identificatie,
       idc.ident_oud                                                        AS was_identificatie,
       sdl.aardstukdeel_code                                                AS aard_code,
       asl.omschrijving                                                     AS aard_omschrijving,
       sdl.bedrtransactiesomlev_vlt_code                                    AS bedrag_transactie_valuta,
       to_char(sdl.bedragtransactiesomlevering, '999999999999.99')::numeric AS bedrag_transactie_bedrag,
       tng.tng_ids                                                          AS is_bron_voor_brk_tenaamstelling,
       akt.akt_ids                                                          AS is_bron_voor_brk_aantekening_kadastraal_object,
       art.art_ids                                                          AS is_bron_voor_brk_aantekening_recht,
       zrt.zrt_ids                                                          AS is_bron_voor_brk_zakelijk_recht,
       ec.ec_ids                                                            AS is_bron_voor_brk_erfpachtcanon,
       stk.identificatie                                                    AS stukidentificatie,
       stk.akrportefeuillenr                                                AS portefeuillenummer_akr,
       stk.tijdstip_aanbieding                                              AS tijdstip_aanbieding_stuk,
       stk.reeks_code                                                       AS reeks_code,
       ree.omschrijving                                                     AS reeks_omschrijving,
       stk.nummer                                                           AS volgnummer_stuk,
       stk.registercode_code                                                AS registercode_stuk_code,
       rce.omschrijving                                                     AS registercode_stuk_omschrijving,
       stk.soortregister_code                                               AS soort_register_stuk_code,
       srr.omschrijving                                                     AS soort_register_stuk_omschrijving,
       stk.deel                                                             AS deel_soort_stuk,
       meta.toestandsdatum                                                  AS toestandsdatum,
       stk.tekeningingeschreven                                             AS tekening_ingeschreven,
       stk.tijdstipondertekening                                            AS tijdstip_ondertekening,
       stk.toelichtingbewaarder                                             AS toelichting_bewaarder,
       CASE
           WHEN tng.max_tng_eind_geldigheid IS NULL OR zrt.max_zrt_eind_geldigheid IS NULL OR
                akt.max_akt_expiration_date IS NULL OR art.max_art_expiration_date IS NULL THEN NULL
           ELSE
               GREATEST(tng.max_tng_eind_geldigheid, zrt.max_zrt_eind_geldigheid, akt.max_akt_expiration_date,
                        art.max_art_expiration_date) END                 AS datum_actueel_tot,
       CASE
           WHEN tng.max_tng_eind_geldigheid IS NULL OR zrt.max_zrt_eind_geldigheid IS NULL OR
                akt.max_akt_expiration_date IS NULL OR art.max_art_expiration_date IS NULL THEN NULL
           ELSE
               GREATEST(tng.max_tng_eind_geldigheid, zrt.max_zrt_eind_geldigheid, akt.max_akt_expiration_date,
                        art.max_art_expiration_date) END                 AS _expiration_date
FROM brk2.stukdeel sdl
         LEFT JOIN brk2.stuk stk ON sdl.stuk_identificatie = stk.identificatie
         LEFT JOIN brk2.c_aardstukdeel asl ON sdl.aardstukdeel_code = asl.code
         LEFT JOIN brk2.c_soortregister srr ON stk.soortregister_code = srr.code
         LEFT JOIN brk2.c_registercode rce ON stk.registercode_code = rce.code
         LEFT JOIN brk2.c_reekscode ree ON stk.reeks_code = ree.code
         LEFT JOIN (SELECT tip.stukdeel_identificatie,
                           MIN(tng.begin_geldigheid)                                                              AS min_tng_begin_geldigheid,
                           MAX(tng.begin_geldigheid)                                                              AS max_tng_begin_geldigheid,
                           CASE
                               WHEN SUM(CASE WHEN tng.eind_geldigheid IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                               ELSE MAX(tng.eind_geldigheid) END                                                  AS max_tng_eind_geldigheid,
                           JSONB_AGG(JSONB_BUILD_OBJECT('tng_identificatie', tng.identificatie, 'tng_neuron_id',
                                                        tng.neuron_id)
                                     ORDER BY tng.identificatie, tng.neuron_id)                                   AS tng_ids
                    FROM brk2.tenaamstelling_isgebaseerdop tip
                             JOIN brk2_prep.tenaamstelling tng ON tng.neuron_id = tip.tenaamstelling_id
                    GROUP BY tip.stukdeel_identificatie) tng ON sdl.identificatie = tng.stukdeel_identificatie
         LEFT JOIN (SELECT q.stukdeel_identificatie,
                           JSONB_AGG(JSONB_BUILD_OBJECT('art_identificatie', q.identificatie, 'art_neuron_id',
                                                        q.neuron_id))  AS art_ids,
                           CASE
                               WHEN SUM(CASE WHEN q.max_art_expiration_date IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                               ELSE MAX(q.max_art_expiration_date) END AS max_art_expiration_date
                    FROM (SELECT atg.stukdeel_identificatie,
                                 atg.identificatie,
                                 art.neuron_id,
                                 CASE
                                     WHEN SUM(CASE WHEN art._expiration_date IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                                     ELSE MAX(art._expiration_date) END AS max_art_expiration_date
                          FROM brk2.aantekening atg
                                   JOIN brk2_prep.aantekening_recht art ON art.neuron_id = atg.id
                          GROUP BY atg.stukdeel_identificatie, atg.identificatie, art.neuron_id) q
                    GROUP BY q.stukdeel_identificatie) art ON sdl.identificatie = art.stukdeel_identificatie
         LEFT JOIN (SELECT q.stukdeel_identificatie,
                           JSONB_AGG(JSONB_BUILD_OBJECT('akt_identificatie', q.identificatie, 'akt_neuron_id',
                                                        q.__neuron_id)) AS akt_ids,
                           CASE
                               WHEN SUM(CASE WHEN q.max_akt_expiration_date IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                               ELSE MAX(q.max_akt_expiration_date) END  AS max_akt_expiration_date
                    FROM (SELECT atg.stukdeel_identificatie,
                                 atg.identificatie,
                                 akt.__neuron_id,
                                 CASE
                                     WHEN SUM(CASE WHEN akt._expiration_date IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                                     ELSE MAX(akt._expiration_date) END AS max_akt_expiration_date
                          FROM brk2.aantekening atg
                                   JOIN brk2_prep.aantekening_kadastraal_object akt ON akt.__neuron_id = atg.id
                          GROUP BY atg.stukdeel_identificatie, atg.identificatie, akt.__neuron_id) q
                    GROUP BY q.stukdeel_identificatie) akt ON sdl.identificatie = akt.stukdeel_identificatie
         LEFT JOIN (SELECT q.stukdeel_identificatie,
                           MIN(min_zrt_begin_geldigheid)             AS min_zrt_begin_geldigheid,
                           CASE
                               WHEN SUM(CASE WHEN max_zrt_eind_geldigheid IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                               ELSE MAX(max_zrt_eind_geldigheid) END AS max_zrt_eind_geldigheid,
                           JSONB_AGG(JSONB_BUILD_OBJECT('zrt_identificatie', zrt_identificatie)
                                     ORDER BY zrt_identificatie)     AS zrt_ids
                    FROM (SELECT asg.stukdeel_identificatie,
                                 MIN(zrt.begin_geldigheid)             AS min_zrt_begin_geldigheid,
                                 zrt.identificatie                     AS zrt_identificatie,
                                 CASE
                                     WHEN SUM(CASE WHEN zrt.eind_geldigheid IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                                     ELSE MAX(zrt.eind_geldigheid) END AS max_zrt_eind_geldigheid
                          FROM brk2.appartementsrechtsplitsing asg
                                   JOIN brk2_prep.zakelijk_recht zrt ON zrt.__ontstaan_uit_asg_id = asg.id
                          GROUP BY asg.stukdeel_identificatie, zrt.identificatie) q
                    GROUP BY q.stukdeel_identificatie) zrt ON sdl.identificatie = zrt.stukdeel_identificatie
         LEFT JOIN (SELECT q.is_gebaseerd_op_brk_stukdeel_identificatie                       AS stukdeel_identificatie
                         , JSONB_AGG(JSONB_BUILD_OBJECT('ec_identificatie', q.identificatie)) AS ec_ids
                         , CASE
                               WHEN SUM(CASE WHEN q.max_ec_expiration_date IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                               ELSE MAX(q.max_ec_expiration_date) END                         AS max_ec_expiration_date
                    FROM (SELECT b.is_gebaseerd_op_brk_stukdeel_identificatie,
                                 b.identificatie,
                                 CASE
                                     WHEN SUM(CASE WHEN b.datum_actueel_tot IS NULL THEN 0 ELSE 1 END) < 1 THEN NULL
                                     ELSE MAX(b.datum_actueel_tot) END AS max_ec_expiration_date
                          FROM brk2_prep.erfpachtcanon b
                          GROUP BY b.is_gebaseerd_op_brk_stukdeel_identificatie, b.identificatie) q
                    GROUP BY q.is_gebaseerd_op_brk_stukdeel_identificatie) ec
                   ON sdl.identificatie = ec.stukdeel_identificatie
         LEFT OUTER JOIN brk2_prep.id_conversion idc ON idc.ident_nieuw = stk.identificatie
         JOIN brk2_prep.meta meta ON TRUE
WHERE COALESCE(
                tng.tng_ids -> 0 -> 'tng_identificatie',
                akt.akt_ids -> 0 -> 'akt_identificatie',
                art.art_ids -> 0 -> 'art_identificatie',
                zrt.zrt_ids -> 0 -> 'zrt_identificatie',
                ec.ec_ids -> 0 -> 'ec_identificatie'
    ) IS NOT NULL
  -- Exclude NL.IMKAD.Stukdeel.33029100 since it has 287.000 relations
  AND sdl.identificatie <> 'NL.IMKAD.Stukdeel.33029100'
;
