SET max_parallel_workers_per_gather = 0;
CREATE TABLE brk2_prep.aantekening_recht USING columnar AS
SELECT atg.id                                                       AS neuron_id,
       atg.identificatie                                            AS identificatie,
       idc.ident_oud                                                AS was_identificatie,
       atg.einddatum_recht::timestamp                               AS einddatum_recht,
       atg.aardaantekening_code                                     AS aard_code,
       aag.omschrijving                                             AS aard_omschrijving,
       atg.omschrijving                                             AS omschrijving,
       atg.betreft_gedeelte_van_perceel                             AS betreft_gedeelte_van_perceel,
       art.tng_ids                                                  AS betrokken_brk_tenaamstelling,
       abn.sjt_identificaties                                       AS heeft_brk_betrokken_persoon,
       atg.stukdeel_identificatie                                   AS is_gebaseerd_op_brk_stukdeel,
       atg.einddatum::timestamp                                     AS einddatum,
       LEAST(atg.einddatum, art.max_tng_eind_geldigheid)::timestamp AS datum_actueel_tot,
       LEAST(atg.einddatum, art.max_tng_eind_geldigheid)::timestamp AS _expiration_date,
       art.toestandsdatum::timestamp                                AS toestandsdatum,
       art.max_tng_begin_geldigheid::timestamp                      AS __max_tng_begin_geldigheid
FROM brk2.aantekening atg
         JOIN (SELECT art.aantekening_identificatie,
                      -- Return NULL for eind_geldigheid if any is NULL
                      CASE
                          WHEN SUM(CASE WHEN tng.eind_geldigheid IS NULL THEN 1 ELSE 0 END)
                              > 0 THEN NULL
                          ELSE MAX(tng.eind_geldigheid) END     AS max_tng_eind_geldigheid,
                      MAX(tng.toestandsdatum)                   AS toestandsdatum,
                      MAX(tng.begin_geldigheid)                 AS max_tng_begin_geldigheid,

                      JSONB_AGG(DISTINCT JSONB_BUILD_OBJECT('tng_identificatie', tng.identificatie) ORDER BY tng.identificatie) AS tng_ids
               FROM brk2.tenaamstelling_aantekening art
                        JOIN brk2_prep.tenaamstelling tng ON art.tenaamstelling_id = tng.neuron_id
               GROUP BY art.aantekening_identificatie) art ON art.aantekening_identificatie = atg.identificatie
         LEFT JOIN brk2.c_aardaantekening aag ON atg.aardaantekening_code = aag.code
         LEFT JOIN (SELECT abn.aantekening_id,
                           JSONB_AGG(
                                   JSONB_BUILD_OBJECT(
                                           'sjt_identificatie', subject_identificatie
                                       ) ORDER BY subject_identificatie
                               ) AS sjt_identificaties
                    FROM brk2.aantekening_betrokkenpersoon abn
                    GROUP BY abn.aantekening_id) abn ON abn.aantekening_id = atg.id
         LEFT OUTER JOIN brk2_prep.id_conversion idc ON idc.ident_nieuw = atg.identificatie
