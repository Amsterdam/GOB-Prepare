SET max_parallel_workers_per_gather = 0;
CREATE TABLE brk2_prep.erfpachtcanon USING columnar AS
SELECT ec.identificatie                                            AS identificatie
     , z.volgnummer                                                AS volgnummer
     , ec.soort_code                                               AS soort_code
     , cs.omschrijving                                             AS soort_omschrijving
     , to_char(ec.jaarlijksbedrag_som, '999999999999.99')::numeric AS jaarlijks_bedrag_valuta_som
     , ec.jaarlijksbedrag_valuta_code                              AS jaarlijks_bedrag_valuta_code
     , ec.jaarlijksbedrag_betreftmeeroz                            AS jaarlijks_bedrag_betreft_meer_onroerende_zaken
     , ec.einddatumafkoop                                          AS einddatum_afkoop
     , ec.indicatie_oude_oz_betrokken                              AS indicatie_oude_onroerende_zaken_betrokken
     , ec.stukdeel_identificatie                                   AS is_gebaseerd_op_brk_stukdeel_identificatie
     , z.identificatie                                             AS betreft_brk_zakelijk_recht_identificatie
     , z.volgnummer                                                AS betreft_brk_zakelijkrecht_volgnummer
     , z."_expiration_date"                                        AS datum_actueel_tot
     , z.toestandsdatum                                            AS toestandsdatum
     , z.begin_geldigheid                                          AS begin_geldigheid
     , z.eind_geldigheid                                           AS eind_geldigheid
FROM brk2.erfpachtcanon ec
         LEFT JOIN brk2.c_soorterfpachtcanon cs
                   ON ec.soort_code = cs.code
         LEFT JOIN brk2.zakelijkrecht_erfpachtcanon ze
                   ON ec.identificatie = ze.erfpachtcanon_identificatie
         LEFT JOIN brk2_prep.zakelijk_recht z
                   ON ze.zakelijkrecht_id = z."__id"
WHERE z.identificatie IS NOT NULL
