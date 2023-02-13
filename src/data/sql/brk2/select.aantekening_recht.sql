SELECT atg.id                                            AS neuron_id,
       atg.identificatie                                 AS identificatie,
       atg.einddatum_recht                               AS einddatum_recht,
       atg.aardaantekening_code                          AS aard_code,
       aag.omschrijving                                  AS aard_omschrijving,
       atg.omschrijving                                  AS omschrijving,
       atg.betreft_gedeelte_van_perceel                  AS betreft_gedeelte_van_perceel,
       art.tng_ids                                       AS betrokken_brk_tenaamstelling,
       abn.sjt_identificaties                            AS heeft_brk_betrokken_persoon,
       atg.stukdeel_identificatie                        AS is_gebaseerd_op_brk_stukdeel,
       atg.einddatum                                     AS einddatum,
       LEAST(atg.einddatum, art.max_tng_eind_geldigheid) AS datum_actueel_tot,
       LEAST(atg.einddatum, art.max_tng_eind_geldigheid) AS _expiration_date,
       art.toestandsdatum                                AS toestandsdatum,
       art.max_tng_begin_geldigheid                      AS __max_tng_begin_geldigheid
FROM brk2.aantekening atg
         JOIN (SELECT art.aantekening_identificatie,
                      -- Return NULL for eind_geldigheid if any is NULL
                      CASE
                          WHEN SUM(CASE WHEN tng.eind_geldigheid IS NULL THEN 1 ELSE 0 END)
                              > 0 THEN NULL
                          ELSE MAX(tng.eind_geldigheid) END     AS max_tng_eind_geldigheid,
                      MAX(tng.toestandsdatum)                   AS toestandsdatum,
                      MAX(tng.begin_geldigheid)                 AS max_tng_begin_geldigheid,
                      JSONB_AGG(JSONB_BUILD_OBJECT('tng_id', tenaamstelling_id)
                                ORDER BY art.tenaamstelling_id) AS tng_ids
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