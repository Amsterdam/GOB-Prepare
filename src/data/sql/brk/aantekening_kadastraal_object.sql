SELECT atg.identificatie                          AS brk_atg_id
   ,atg.id                                     AS nrn_atg_id
   ,atg.aardaantekening_code                   AS atg_aardaantekening_code
   ,aag.omschrijving                           AS atg_aardaantekening_oms
   ,atg.omschrijving                           AS atg_omschrijving
   ,atg.einddatum                              AS atg_einddatum
   ,kot.identificatie                          AS brk_kot_id -- BETREKKING OP KOT
   ,kot.id                                     AS nrn_kot_id -- BETREKKING OP KOT
   ,kot.volgnummer                             AS nrn_kot_volgnr -- BETREKKING OP KOT
   ,abn.subject_identificatie                  AS brk_sjt_id -- BETREKKING OP ZRT
   ,geb.stukdeel_identificatie                 AS brk_sdl_id
   ,bsd.brk_bsd_toestandsdatum                 AS toestandsdatum
   ,LEAST(
    CASE
        WHEN kot.modification IS NOT NULL THEN kot.modification
        ELSE (
            CASE kot.status_code
                WHEN 'H' THEN kot.creation
                ELSE NULL
            END
        )
    END,
    atg.einddatum
) 											AS expiration_date
--
FROM   brk.aantekening atg
JOIN   brk.aantekening_kadastraalobject akt             ON     (atg.id = akt.aantekening_id)
JOIN   brk.kadastraal_object kot                        ON     (akt.kadastraalobject_id = kot.id AND
                                                             akt.kadastraalobject_volgnummer = kot.volgnummer)
LEFT   JOIN brk.aantekeningbetrokkenpersoon abn         ON     (atg.id = abn.aantekening_id)
LEFT   JOIN BRK.AANTEKENINGISGEBASEERDOP geb            ON     (atg.id=geb.aantekening_id)
JOIN   brk.c_aardaantekening aag                        ON     (atg.aardaantekening_code = aag.code)
JOIN   brk.bestand bsd                                  ON     (1 = 1)