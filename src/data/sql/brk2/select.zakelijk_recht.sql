CREATE TABLE brk2_prep.zakelijk_recht AS
WITH asg_codes(code, omschrijving) AS (VALUES (1, 'HoofdSplitsing'),
                                              (2, 'OnderSplitsing'),
                                              (3, 'SplitsingAfkoopErfpacht')),
     akr_codes(aard_code, akr_code) AS (VALUES ('1', 'BK'),
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
SELECT zrt.rust_op_kadastraalobj_volgnr  AS volgnummer,
       NULL                              AS registratiedatum,
       zrt.id                            AS __id,
       zrt.identificatie                 AS identificatie,
       bel.belast                        AS belast_zakelijkerechten,
       blm.is_belast_met                 AS belast_met_zakelijkerechten,
       NULL                              AS ontstaan_uit_zakelijkerechten,
       zrt.isontstaanuit_identificatie   AS ontstaan_uit_appartementsrechtsplitsing_vve,
       asg1.id                           AS __betrokken_bij_asg_id,
       asg2.id                           AS __ontstaan_uit_asg_id,
       NULL                              AS betrokken_bij_zakelijkerechten,
       zrt.isbetrokkenbij_identificatie  AS betrokken_bij_appartementsrechtsplitsing_vve,
       ztt.isbeperkttot_id               AS is_beperkt_tot,
       NULL                              AS appartementsrechtsplitsingidentificatie,
       asg1.vve_identificatie            AS vve_identificatie_ontstaan_uit,
       asg2.vve_identificatie            AS vve_identificatie_betrokken_bij,
       nullif(array_to_string(array [asg2.identificatie, asg1.identificatie], ','),
              '')                        AS appartementsrechtsplitsingtype_code,
       nullif(array_to_string(array [ase2.omschrijving, ase1.omschrijving], ','),
              '')                        AS appartementsrechtsplitsingtype_omschrijving,
       zrt.isbestemdtot_identificatie    AS isbestemdtot_identificatie,
       zrt.toelichting_bewaarder         AS toelichting_bewaarder,
       agn.omschrijving                  AS inonderzoek,
       zrt.aardzakelijkrecht_code        AS aard_zakelijk_recht_code,
       a.omschrijving                    AS aard_zakelijk_recht_omschrijving,
       COALESCE(akr_codes.akr_code, '?') AS akr_aard_zakelijk_recht,
       kot.identificatie                 AS rust_op_kadastraalobject,
       kot.id                            AS __rust_op_kot_id,
       kot.volgnummer                    AS __rust_op_kot_volgnummer,
       kot.begin_geldigheid              AS begin_geldigheid,
       kot.eind_geldigheid               AS eind_geldigheid,
       kot.toestandsdatum                AS toestandsdatum,
       kot.datum_actueel_tot             AS datum_actueel_tot,
       kot._expiration_date              AS _expiration_date,
       NULL                              AS __max_betrokken_bij_begindatum,
       NULL                              AS __max_ontstaan_uit_begindatum
FROM brk2.zakelijkrecht zrt
         LEFT JOIN brk2.zakelijkrecht_isbeperkttot ztt ON zrt.id = ztt.zakelijkrecht_id
         LEFT JOIN brk2.c_aardzakelijkrecht a ON zrt.aardzakelijkrecht_code = a.code
         LEFT JOIN brk2_prep.kadastraal_object kot
                   ON zrt.rust_op_kadastraalobject_id = kot.id AND zrt.rust_op_kadastraalobj_volgnr = kot.volgnummer
         LEFT JOIN brk2.appartementsrechtsplitsing asg1 ON asg1.identificatie = zrt.isbetrokkenbij_identificatie
         LEFT JOIN brk2.appartementsrechtsplitsing asg2 ON asg2.identificatie = zrt.isontstaanuit_identificatie
         LEFT JOIN asg_codes ase1 ON ase1.code = asg1.splitsingstype
         LEFT JOIN asg_codes ase2 ON ase2.code = asg2.splitsingstype

         LEFT JOIN brk2.zakelijkrecht_onderzoek zok ON zok.zakelijkrecht_id = zrt.id
         LEFT JOIN brk2.inonderzoek iok ON iok.identificatie = zok.onderzoek_identificatie
         LEFT JOIN brk2.c_authentiekgegeven agn ON agn.code = iok.authentiekgegeven_code
         LEFT JOIN akr_codes ON zrt.aardzakelijkrecht_code = akr_codes.aard_code
         LEFT JOIN (SELECT zrt_id,
                           array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie)
                                                   ORDER BY identificatie)) as is_belast_met
                    FROM (SELECT zit.zakelijkrecht_id AS zrt_id,       --blijft
                                 zrt2.identificatie   AS identificatie --blijft
                          FROM brk2.zakelijkrecht_isbelastmet zit --blijft
                                   LEFT JOIN brk2.zakelijkrecht zrt2 --blijft
                                             ON zrt2.id = zit.isbelastmet_id) sq
                    GROUP BY zrt_id) blm ON blm.zrt_id = zrt.id
         LEFT JOIN (SELECT zrt_id,
                           array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie)
                                                   ORDER BY identificatie)) as belast
                    FROM (SELECT zit.isbelastmet_id AS zrt_id,
                                 zrt2.identificatie AS identificatie
                          FROM brk2.zakelijkrecht_isbelastmet zit
                                   LEFT JOIN brk2.zakelijkrecht zrt2
                                             ON zrt2.id = zit.zakelijkrecht_id) sq
                    GROUP BY zrt_id) bel ON bel.zrt_id = zrt.id
;
