-- Query assumes brk2_prep.zakelijk_recht is fully populated and ready.
-- Volgnummer, begindatum and einddatum are taken from ZRT; ZRT in turn gets these from KOT: this is how it works in
-- the source database.
-- We get these attributes from the brk2_prep.zakelijk_recht table instead of from the kadastraalobject table from the
-- brk schema, because the kadastraal object references aren't populated on all ZRT objects in the brk schema.
SET max_parallel_workers_per_gather = 0;
CREATE TABLE brk2_prep.tenaamstelling USING columnar AS
SELECT tng.identificatie                                     AS identificatie,
       tng.id                                                AS neuron_id,
       zrt.volgnummer                                        AS volgnummer,
       idc.ident_oud                                         AS was_identificatie,
       tng.tennamevan_identificatie                          AS van_brk_kadastraalsubject,
       zrt.begin_geldigheid::timestamp                       AS begin_geldigheid,
       LEAST(zrt._expiration_date, atg.einddatum)::timestamp AS eind_geldigheid,
       LEAST(zrt._expiration_date, atg.einddatum)::timestamp AS datum_actueel_tot,
       LEAST(zrt._expiration_date, atg.einddatum)::timestamp AS _expiration_date,
       tng.aandeel_teller                                    AS aandeel_teller,
       tng.aandeel_noemer                                    AS aandeel_noemer,
       ga.teller                                             AS geldt_voor_teller,
       ga.noemer                                             AS geldt_voor_noemer,
       tng.burgerlijkestaat_code                             AS burgerlijke_staat_ten_tijde_van_verkrijging_code,
       b.omschrijving                                        AS burgerlijke_staat_ten_tijde_van_verkrijging_omschrijving,
       tng.betrokkenpartner_identificatie                    AS betrokken_partner_brk_subject,
       tng.betrokkensvb_identificatie                        AS betrokken_samenwerkingsverband_brk_subject,
       tng.verkregen_namens_code                             AS verkregen_namens_samenwerkingsverband_type,
       s.omschrijving                                        AS verkregen_namens_samenwerkingsverband_omschrijving,
       tng.betrokkengorswas_identificatie                    AS betrokken_gorzen_en_aanwassen_brk_subject,
       ag.omschrijving                                       AS in_onderzoek,
       zrt.identificatie                                     AS van_brk_zakelijk_recht,
       g.isgebaseerd_op                                      AS is_gebaseerd_op_brk_stukdeel,
       zrt.toestandsdatum::timestamp                         AS toestandsdatum
FROM brk2.tenaamstelling tng
         JOIN brk2_prep.zakelijk_recht zrt ON tng.vanrecht_id = zrt.__id
         LEFT JOIN brk2.gezamenlijk_aandeel ga ON tng.geldtvoordeel_identificatie = ga.identificatie
         LEFT JOIN brk2.c_samenwerkingsverband s ON s.code = tng.verkregen_namens_code
         LEFT JOIN brk2.c_burgerlijkestaat b ON b.code = tng.burgerlijkestaat_code
         LEFT JOIN (SELECT g.tenaamstelling_id,
                           JSONB_AGG(JSONB_BUILD_OBJECT(
                                   'bronwaarde', stukdeel_identificatie
                               ) ORDER BY stukdeel_identificatie) isgebaseerd_op
                    FROM brk2.tenaamstelling_isgebaseerdop g
                    GROUP BY g.tenaamstelling_id) g ON tng.id = g.tenaamstelling_id
         LEFT JOIN brk2.tenaamstelling_onderzoek o ON tng.id = o.tenaamstelling_id
         LEFT JOIN brk2.inonderzoek io ON io.identificatie = o.onderzoek_identificatie
         LEFT JOIN brk2.c_authentiekgegeven ag ON io.authentiekgegeven_code = ag.code
         LEFT JOIN (SELECT tag.tenaamstelling_id,
                           MAX(atg.id) AS id
                    FROM brk2.tenaamstelling_aantekening tag
                             JOIN brk2.aantekening atg
                                  ON atg.identificatie = tag.aantekening_identificatie
                    WHERE atg.aardaantekening_code = '21'
                    GROUP BY tag.tenaamstelling_id) art ON tng.id = art.tenaamstelling_id
         LEFT JOIN brk2.aantekening atg ON atg.id = art.id
         LEFT OUTER JOIN brk2_prep.id_conversion idc ON idc.ident_nieuw = tng.identificatie
WHERE NOT (tng.identificatie = 'NL.IMKAD.Tenaamstelling.AKR2.100000010664394' AND zrt.volgnummer = 1);
