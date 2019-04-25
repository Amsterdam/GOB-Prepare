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
,bsd.brk_bsd_toestandsdatum       AS toestandsdatum

FROM BRK.AANTEKENING atg
JOIN BRK.AANTEKENINGRECHT art                   ON (atg.id=art.aantekening_id)
LEFT   JOIN brk.aantekeningbetrokkenpersoon abn ON (atg.id = abn.aantekening_id)
JOIN brk.c_aardaantekening aag                  ON (atg.aardaantekening_code = aag.code)
JOIN   brk.bestand bsd                          ON (1 = 1)