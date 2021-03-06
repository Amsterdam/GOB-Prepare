SELECT
 atg.identificatie                 AS brk_atg_id
,atg.id                           AS nrn_atg_id
,atg.aardaantekening_code         AS atg_aardaantekening_code
,aag.omschrijving                 AS atg_aardaantekening_oms
,atg.omschrijving                 AS atg_omschrijving
,atg.einddatum                    AS atg_einddatum
,art.tng_begindatum               AS max_tng_begindatum -- used for partial import
,'Aantekening Zakelijk Recht (R)' AS atg_type
,abn.brk_sjt_ids        		  AS brk_sjt_ids
,art.nrn_tng_ids				  AS nrn_tng_ids
,geb.nrn_sdl_ids 				  AS nrn_sdl_ids
,art.toestandsdatum       AS toestandsdatum
,least(atg.einddatum, art.einddatum)                    AS expiration_date
FROM BRK.AANTEKENING atg
LEFT JOIN (
	SELECT
		abn.aantekening_id,
		array_to_json(array_agg(json_build_object('brk_sjt_id', subject_identificatie) ORDER BY subject_identificatie)) AS brk_sjt_ids
	FROM brk.aantekeningbetrokkenpersoon abn
	GROUP BY abn.aantekening_id
) abn ON (abn.aantekening_id=atg.id)
LEFT JOIN (
	SELECT
		geb.aantekening_id,
		array_to_json(array_agg(json_build_object('nrn_sdl_id', sdl.id) ORDER BY sdl.id)) AS nrn_sdl_ids
	FROM brk.aantekeningisgebaseerdop geb
	JOIN brk.stukdeel sdl
	ON sdl.identificatie=geb.stukdeel_identificatie
	GROUP BY geb.aantekening_id
) geb ON (geb.aantekening_id=atg.id)
JOIN (
	SELECT
		art.aantekening_id,
	    case when sum (case when tng.einddatum is null then 1 else 0 end) > 0 then null else max(tng.einddatum) end AS einddatum,
	    max(tng.toestandsdatum) AS toestandsdatum,
	    max(tng.begindatum) AS tng_begindatum,
		array_to_json(array_agg(json_build_object('nrn_tng_id', tng.nrn_tng_id) ORDER BY tng.nrn_tng_id)) AS nrn_tng_ids
	FROM brk.aantekeningrecht art
	JOIN brk_prep.tenaamstelling tng
	ON art.tenaamstelling_identificatie=tng.brk_tng_id
	GROUP BY art.aantekening_id
) art ON (atg.id=art.aantekening_id)
JOIN brk.c_aardaantekening aag                  ON (atg.aardaantekening_code = aag.code);
