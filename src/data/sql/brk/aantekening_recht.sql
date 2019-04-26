SELECT
 atg.identificatie                 AS brk_atg_id
,atg.id                           AS nrn_atg_id
,atg.aardaantekening_code         AS atg_aardaantekening_code
,aag.omschrijving                 AS atg_aardaantekening_oms
,atg.omschrijving                 AS atg_omschrijving
,atg.einddatum                    AS atg_einddatum
,'Aantekening Zakelijk Recht (R)' AS atg_type
,abn.subject_identificatie        AS brk_sjt_id
,art.tenaamstelling_identificatie AS nrn_tng_id
,geb.stukdeel_identificatie       AS brk_sdl_id
,bsd.brk_bsd_toestandsdatum       AS toestandsdatum
-- select LEAST of kot einddatum ans atg.einddatum, or NULL if both are NULL
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
)                                AS expiration_date
FROM BRK.AANTEKENING atg
JOIN BRK.AANTEKENINGRECHT art                   ON (atg.id=art.aantekening_id)
LEFT   JOIN brk.aantekeningbetrokkenpersoon abn ON (atg.id = abn.aantekening_id)
LEFT JOIN BRK.AANTEKENINGISGEBASEERDOP geb      ON (atg.id=geb.aantekening_id)
JOIN brk.c_aardaantekening aag                  ON (atg.aardaantekening_code = aag.code)
JOIN   brk.bestand bsd                          ON (1 = 1)
--
JOIN brk.TENAAMSTELLING		tng	ON art.TENAAMSTELLING_IDENTIFICATIE=tng.IDENTIFICATIE
JOIN brk.ZAKELIJKRECHT		zrt ON tng.van_id = zrt.ID
JOIN brk.KADASTRAAL_OBJECT	kot ON kot.id=zrt.RUST_OP_KADASTRAALOBJECT_ID AND kot.VOLGNUMMER=zrt.RUST_OP_KADASTRAALOBJ_VOLGNR