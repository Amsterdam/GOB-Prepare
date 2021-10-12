SELECT atg.identificatie                         AS brk_atg_id
     , atg.id                                    AS nrn_atg_id
     , atg.aardaantekening_code                  AS atg_aardaantekening_code
     , aag.omschrijving                          AS atg_aardaantekening_oms
     , atg.omschrijving                          AS atg_omschrijving
     , atg.einddatum                             AS atg_einddatum
     , kot.brk_kot_id                            AS brk_kot_id     -- BETREKKING OP KOT
     , kot.nrn_kot_id                            AS nrn_kot_id     -- BETREKKING OP KOT
     , kot.nrn_kot_volgnr                        AS nrn_kot_volgnr -- BETREKKING OP KOT
     , abn.brk_sjt_ids                           AS brk_sjt_ids
     , geb.nrn_sdl_ids                           AS nrn_sdl_ids
     , kot.toestandsdatum                        AS toestandsdatum
     , kot.creation                              AS begindatum
     , kot.einddatum                             AS einddatum
     , LEAST(kot.expiration_date, atg.einddatum) AS expiration_date
--
FROM brk.aantekening atg

JOIN brk.aantekening_kadastraalobject akt
    ON atg.id = akt.aantekening_id

JOIN brk_prep.kadastraal_object kot
    ON akt.kadastraalobject_id = kot.nrn_kot_id AND akt.kadastraalobject_volgnummer = kot.nrn_kot_volgnr

LEFT JOIN (
    SELECT abn.aantekening_id,
           array_to_json(array_agg(json_build_object('brk_sjt_id', subject_identificatie)
                                   ORDER BY subject_identificatie)) AS brk_sjt_ids
    FROM brk.aantekeningbetrokkenpersoon abn
    GROUP BY abn.aantekening_id
) abn
    ON abn.aantekening_id = atg.id

LEFT JOIN (
    SELECT geb.aantekening_id,
           array_to_json(array_agg(json_build_object('nrn_sdl_id', sdl.id) ORDER BY sdl.id)) AS nrn_sdl_ids
    FROM brk.aantekeningisgebaseerdop geb
    JOIN brk.stukdeel sdl
        ON sdl.identificatie = geb.stukdeel_identificatie
    GROUP BY geb.aantekening_id
) geb
    ON geb.aantekening_id = atg.id

-- Only include aantekeningen which have an aardaantekening_code
JOIN brk.import_aardaantekening aag
    ON atg.aardaantekening_code = aag.code

-- Filter all aantekeningen based on aardaantekening != Aantekening PB
-- https://dev.azure.com/CloudCompetenceCenter/Datateam%20Basis%20en%20Kernregistraties/_workitems/edit/17723
WHERE aag.type != 'Aantekening PB'
