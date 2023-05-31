WITH akr_codes(aard_code, akr_code) AS (VALUES ('1', 'BK'),
                                               ('2', 'VE'),
                                               ('3', 'EP'),
                                               ('4', 'GB'),
                                               ('5', 'GR'),
                                               ('7', 'OS'),
                                               ('9', 'OVR'),
                                               ('10', 'BP'),
                                               ('11', 'SM'),
                                               ('12', 'VG'),
                                               ('13', 'EO'),
                                               ('14', 'OL'),
                                               ('18', 'OV'),
                                               ('20', 'AA'),
                                               ('21', 'BB'),
                                               ('22', 'BR'),
                                               ('23', 'OLG'),
                                               ('24', 'BPG'))
SELECT zrt_kot.volgnummer                                       AS volgnummer,
       zrt.id                                                   AS __id,
       zrt.identificatie                                        AS identificatie,
       idc.ident_oud                                            AS was_identificatie,
       bel.belast                                               AS belast_zakelijkerechten,
       blm.is_belast_met                                        AS belast_met_zakelijkerechten,
       ontstaan_uit.ontstaan_uit_brk_zakelijke_rechten::jsonb   AS ontstaan_uit_brk_zakelijke_rechten,
       zrt.isontstaanuit_identificatie                          AS ontstaan_uit_appartementsrechtsplitsing_vve,
       zrt_asg.__betrokken_bij_asg_id                           AS __betrokken_bij_asg_id,
       zrt_asg.__ontstaan_uit_asg_id                            AS __ontstaan_uit_asg_id,
       betrokken_bij.betrokken_bij_brk_zakelijke_rechten::jsonb AS betrokken_bij_brk_zakelijke_rechten,
       zrt.isbetrokkenbij_identificatie                         AS betrokken_bij_appartementsrechtsplitsing_vve,
       ztt.is_beperkt_tot                                       AS is_beperkt_tot_brk_tenaamstellingen,
       zrt_asg.vve_identificatie_betrokken_bij                  AS vve_identificatie_betrokken_bij,
       zrt_asg.vve_identificatie_ontstaan_uit                   AS vve_identificatie_ontstaan_uit,
       zrt_asg.appartementsrechtsplitsingtype_code              AS appartementsrechtsplitsingtype_code,
       zrt_asg.appartementsrechtsplitsingtype_omschrijving      AS appartementsrechtsplitsingtype_omschrijving,
       zrt.isbestemdtot_identificatie                           AS isbestemdtot_identificatie,
       zrt.toelichting_bewaarder                                AS toelichting_bewaarder,
       agn.omschrijving                                         AS inonderzoek,
       zrt.aardzakelijkrecht_code                               AS aard_zakelijk_recht_code,
       a.omschrijving                                           AS aard_zakelijk_recht_omschrijving,
       COALESCE(akr_codes.akr_code, '?')                        AS akr_aard_zakelijk_recht,
       zrt_kot.rust_op_kadastraalobject                         AS rust_op_kadastraalobject,
       zrt_kot.__rust_op_kot_id                                 AS __rust_op_kot_id,
       zrt_kot.__rust_op_kot_volgnummer                         AS __rust_op_kot_volgnummer,
       zrt_kot.begin_geldigheid                                 AS begin_geldigheid,
       zrt_kot.eind_geldigheid                                  AS eind_geldigheid,
       zrt_kot.toestandsdatum                                   AS toestandsdatum,
       zrt_kot.datum_actueel_tot                                AS datum_actueel_tot,
       zrt_kot._expiration_date                                 AS _expiration_date,
       betrokken_bij.__max_betrokken_bij_begindatum             AS __max_betrokken_bij_begindatum,
       ontstaan_uit.__max_ontstaan_uit_begindatum               AS __max_ontstaan_uit_begindatum
FROM brk2.zakelijkrecht zrt
         JOIN brk2_prep.zrt_kot zrt_kot -- INNER JOIN so that we only import ZRT's with KOT's with status 'B'
              ON zrt.id = zrt_kot.id
         LEFT JOIN brk2_prep.zrt_asg zrt_asg
                   ON zrt.id = zrt_asg.id
         LEFT JOIN (SELECT ztt.zakelijkrecht_id,
                           JSONB_AGG(JSONB_BUILD_OBJECT('bronwaarde', tng.identificatie)
                                     ORDER BY tng.identificatie) AS is_beperkt_tot
                    FROM brk2.zakelijkrecht_isbeperkttot ztt
                             JOIN brk2.tenaamstelling tng ON tng.id = ztt.isbeperkttot_id
                    GROUP BY ztt.zakelijkrecht_id) ztt ON ztt.zakelijkrecht_id = zrt.id
         LEFT JOIN brk2.c_aardzakelijkrecht a ON zrt.aardzakelijkrecht_code = a.code
         LEFT JOIN brk2.zakelijkrecht_onderzoek zok ON zok.zakelijkrecht_id = zrt.id
         LEFT JOIN brk2.inonderzoek iok ON iok.identificatie = zok.onderzoek_identificatie
         LEFT JOIN brk2.c_authentiekgegeven agn ON agn.code = iok.authentiekgegeven_code
         LEFT JOIN akr_codes ON zrt.aardzakelijkrecht_code = akr_codes.aard_code
         LEFT JOIN (SELECT zit.zakelijkrecht_id                  AS zrt_id,
                           JSONB_AGG(JSONB_BUILD_OBJECT('bronwaarde', zrt.identificatie)
                                     ORDER BY zrt.identificatie) AS is_belast_met
                    FROM brk2.zakelijkrecht_isbelastmet zit
                             LEFT JOIN brk2.zakelijkrecht zrt ON zrt.id = zit.isbelastmet_id
                    GROUP BY zit.zakelijkrecht_id) blm ON blm.zrt_id = zrt.id
         LEFT JOIN (SELECT zit.isbelastmet_id                    AS zrt_id,
                           JSONB_AGG(JSONB_BUILD_OBJECT('bronwaarde', zrt.identificatie)
                                     ORDER BY zrt.identificatie) AS belast
                    FROM brk2.zakelijkrecht_isbelastmet zit
                             LEFT JOIN brk2.zakelijkrecht zrt ON zrt.id = zit.zakelijkrecht_id
                    GROUP BY zit.isbelastmet_id) bel ON bel.zrt_id = zrt.id
         LEFT JOIN (SELECT JSONB_AGG(JSONB_BUILD_OBJECT('zrt_identificatie', identificatie)
                                     ORDER BY identificatie) betrokken_bij_brk_zakelijke_rechten,
                           MAX(zrt_begindatum)               __max_betrokken_bij_begindatum,
                           __ontstaan_uit_asg_id
                    FROM (SELECT identificatie,
                                 zrt_asg.__ontstaan_uit_asg_id,
                                 MAX(zrt_asg.begin_geldigheid) zrt_begindatum
                          FROM brk2_prep.zrt_asg zrt_asg
                          GROUP BY identificatie, zrt_asg.__ontstaan_uit_asg_id) q
                    GROUP BY __ontstaan_uit_asg_id) betrokken_bij
                   ON betrokken_bij.__ontstaan_uit_asg_id = zrt_asg.__betrokken_bij_asg_id
         LEFT JOIN (SELECT JSONB_AGG(
                                   JSONB_BUILD_OBJECT('zrt_identificatie', identificatie)
                                   ORDER BY identificatie) ontstaan_uit_brk_zakelijke_rechten,
                           MAX(zrt_begindatum)             __max_ontstaan_uit_begindatum,
                           __betrokken_bij_asg_id
                    FROM (SELECT identificatie,
                                 zrt_asg.__betrokken_bij_asg_id,
                                 MAX(zrt_asg.begin_geldigheid) zrt_begindatum
                          FROM brk2_prep.zrt_asg zrt_asg
                          GROUP BY identificatie, zrt_asg.__betrokken_bij_asg_id) q
                    GROUP BY __betrokken_bij_asg_id) ontstaan_uit
                   ON ontstaan_uit.__betrokken_bij_asg_id = zrt_asg.__ontstaan_uit_asg_id
         LEFT OUTER JOIN brk2_prep.id_conversion idc ON idc.ident_nieuw = zrt.identificatie
;

CREATE INDEX ON brk2_prep.zakelijk_recht (__id);
CREATE INDEX ON brk2_prep.zakelijk_recht (__rust_op_kot_id, __rust_op_kot_volgnummer);
CREATE INDEX ON brk2_prep.zakelijk_recht (betrokken_bij_appartementsrechtsplitsing_vve);
