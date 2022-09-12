SELECT atg.identificatie											AS brk_atg_id
,atg.id  															AS nrn_atg_id
,atg.aardaantekening_code 											AS atg_aardaantekening_code
,aag.omschrijving													AS atg_aardaantekening_oms
,atg.omschrijving													AS atg_omschrijving
,atg.einddatum   							                        AS atg_einddatum
,atg.einddatum_recht												AS atg_eindatum_recht					--jr, 2022-09-09: nieuw 
,ko.identificatie 													AS brk_kot_id
,koa.kadastraalobject_id 											AS nrn_kot_id
,koa.kadastraalobject_volgnummer  									AS nrn_kot_volgnr
,ko.toestandsdatum    												AS toestandsdatum
,ko.begin_geldigheid												AS begindatum
,ko.eind_geldigheid                             					AS einddatum
,LEAST(ko."_expiration_date" , atg.einddatum, atg.einddatum_recht) 	AS expiration_date
,atg.betreft_gedeelte_van_perceel									AS atg_betreft_gedeelte_van_perceel		--jr, 2022-09-09: nieuw 
,atg.stukdeel_identificatie											AS atg_stukdeel_identificatie			--jr, 2022-09-09: nieuw, was multi (nrn_sdl_ids), is nu enkel geworden. kan ook leeg zijn. Vreemd.
,abn.brk_sjt_ids													AS brk_sjt_ids
--   geb.nrn_sdl_ids                           AS nrn_sdl_ids 												--jr, 2022-09-12: vervallen, nu enkel (atg_stukdeel_identificatie)
FROM brk2_jeroen_speeltuin.aantekening atg
	LEFT JOIN brk2_jeroen_speeltuin.aantekening_betrokkenpersoon abp ON abp.aantekening_id =atg.id 
	JOIN brk2_jeroen_speeltuin.kadastraal_object_aantekening koa ON koa.aantekening_identificatie =atg.identificatie 
	LEFT JOIN c_aardaantekening aag ON atg.aardaantekening_code =aag.code
	LEFT JOIN brk2_prepared.kadastraal_object ko ON (ko.id  =koa.kadastraalobject_id AND ko.volgnummer =koa.kadastraalobject_volgnummer)
    LEFT JOIN (
		SELECT apn.aantekening_id,
		array_to_json(array_agg(json_build_object('brk_sjt_id', subject_identificatie)
		ORDER BY subject_identificatie)) AS brk_sjt_ids
		FROM brk2_jeroen_speeltuin.aantekening_betrokkenpersoon apn
		GROUP BY apn.aantekening_id
		) abn ON (abn.aantekening_id = atg.id)	
/*-- TEMPORARY Filter duplicate identification values (31x brk_atg_id)
where atg.identificatie not in (
    select distinct a.brk_atg_id
    from (
             select brk_atg_id, count(brk_atg_id)
             from brk_prep.aantekening_kadastraal_object ako
             group by brk_atg_id, nrn_kot_volgnr
             having count(*) > 1
         ) a
)*/ --jr, 2022-09-09: hopelijk niet meer nodig, nog niet gechecked
