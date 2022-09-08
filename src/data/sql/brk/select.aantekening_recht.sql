CREATE TABLE brk2_jeroen_speeltuin.prep_aantekeningrecht AS
SELECT	atg.identificatie			AS brk_atg_id
, atg.id							AS nrn_atg_id
, atg.aardaantekening_code			AS atg_aardaantekening_code
, aag.omschrijving					AS atg_aardaantekening_oms
, atg.omschrijving					AS atg_omschrijving
, atg.einddatum						AS atg_einddatum
, atg.einddatum_recht				AS einddatum_recht				--jr,2022-09-02: nieuw
, atg.betreft_gedeelte_van_perceel	AS betreft_gedeelte_van_perceel --jr,2022-09-02: nieuw
, atg.stukdeel_identificatie		AS stukdeel_identificatie		--jr,2022-09-02: nieuw, vervangt de geb-JOIN, denk ik, lijkt nu ook enkel te zijn, dus geen json meer hier
, com.tng_begindatum				AS max_tng_begindatum			--used for partial import
,'Aantekening Zakelijk Recht (R)'	AS atg_type
, abn.brk_sjt_ids					AS brk_sjt_ids
, com.tng_ids						AS nrn_tng_ids
--,geb.nrn_sdl_ids		AS nrn_sdl_ids							--jr,2022-09-02: kan uit, vervangen door stukdeel_identificatie (denk ik)
, com.toestandsdatum					AS toestandsdatum
, least(atg.einddatum, com.einddatum) AS expiration_date
FROM brk2_jeroen_speeltuin.AANTEKENING atg
LEFT JOIN
		(SELECT
		abn.aantekening_id
		, array_to_json(array_agg(json_build_object('brk_sjt_id', subject_identificatie) ORDER BY subject_identificatie)) AS brk_sjt_ids
		FROM
		brk2_jeroen_speeltuin.aantekening_betrokkenpersoon abn
		GROUP BY
		abn.aantekening_id
		) abn 
ON(abn.aantekening_id=atg.id)
	/*LEFT JOIN (							--jr,2022-09-0: onderstaand is volgens mij overbodig geworden
		SELECT
		geb.aantekening_id,
		array_to_json(array_agg(json_build_object('nrn_sdl_id', sdl.id) ORDER BY sdl.id)) AS nrn_sdl_ids
		FROM brk.aantekeningisgebaseerdop geb
		JOIN brk.stukdeel sdl
		ON sdl.identificatie=geb.stukdeel_identificatie
		GROUP BY geb.aantekening_id
		) geb 
	ON (geb.aantekening_id=atg.id)*/
JOIN(SELECT
	art.aantekening_identificatie																					AS aantekening_identificatie
	,CASE WHEN SUM(CASE WHEN tng.einddatum IS NULL THEN 1 ELSE 0 END)
					> 0 THEN NULL ELSE MAX(tng.einddatum) END														AS einddatum
	,MAX(tng.toestandsdatum)																						AS toestandsdatum
	,MAX(tng.begindatum)																							AS tng_begindatum
					  --,array_to_json(array_agg(json_build_object('nrn_tng_id', tng.nrn_tng_id) ORDER BY tng.nrn_tng_id))			AS nrn_tng_ids
						,array_to_json(array_agg(json_build_object('nrn_tng_id', tenaamstelling_id) ORDER BY art.tenaamstelling_id))	AS tng_ids --jr,2022-09-02 :andere JOIN, andere naam, maar nog steeds een neuron-id
	FROM brk2_jeroen_speeltuin.tenaamstelling_aantekening art
	JOIN brk2_jeroen_speeltuin.prep_tenaamstelling tng
	ON art.tenaamstelling_id=tng.nrn_tng_id 
	GROUP BY art.aantekening_identificatie) com
ON (atg.identificatie=com.aantekening_identificatie)
JOIN brk2_jeroen_speeltuin.c_aardaantekening aag
ON (atg.aardaantekening_code = aag.code)