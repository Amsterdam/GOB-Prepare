SELECT atg.identificatie                                               AS identificatie,
       atg.id                                                          AS __neuron_id,
       koa.kadastraalobject_volgnummer                                 AS volgnummer,
       NULL                                                            AS registratiedatum,
       kot.begin_geldigheid                                            AS begin_geldigheid,
       kot.eind_geldigheid                                             AS eind_geldigheid,
       atg.einddatum_recht                                             AS einddatum_recht,
       atg.aardaantekening_code                                        AS aard_code,
       aag.omschrijving                                                AS aard_omschrijving,
       atg.omschrijving                                                AS omschrijving,
       atg.betreft_gedeelte_van_perceel                                AS betreft_gedeelte_van_perceel,
       abn.sjt_identificaties                                          AS heeft_brk_betrokken_persoon,
       kot.identificatie                                               AS heeft_betrekking_op_brk_kadastraal_object,
       atg.stukdeel_identificatie                                      AS is_gebaseerd_op_brk_stukdeel,
       atg.einddatum                                                   AS einddatum,
       LEAST(kot._expiration_date, atg.einddatum, atg.einddatum_recht) AS datum_actueel_tot,
       LEAST(kot._expiration_date, atg.einddatum, atg.einddatum_recht) AS _expiration_date,
       kot.toestandsdatum                                              AS toestandsdatum
FROM brk2.aantekening atg
         JOIN brk2.kadastraal_object_aantekening koa ON koa.aantekening_identificatie = atg.identificatie
    -- Filter all aantekeningen based on aardaantekening != Aantekening PB
    -- https://dev.azure.com/CloudCompetenceCenter/Datateam%20Basis%20en%20Kernregistraties/_workitems/edit/17723
         JOIN (SELECT * FROM brk2.import_aardaantekening aag WHERE aag.type != 'Aantekening PB') aag
              ON atg.aardaantekening_code = aag.code
         JOIN brk2_prep.kadastraal_object kot
                   ON kot.id = koa.kadastraalobject_id AND
                      kot.volgnummer = koa.kadastraalobject_volgnummer
         LEFT JOIN (SELECT abn.aantekening_id,
                           JSONB_AGG(
                                   JSONB_BUILD_OBJECT(
                                           'sjt_identificatie', subject_identificatie
                                       ) ORDER BY subject_identificatie
                               ) AS sjt_identificaties
                    FROM brk2.aantekening_betrokkenpersoon abn
                    GROUP BY abn.aantekening_id) abn ON abn.aantekening_id = atg.id
;