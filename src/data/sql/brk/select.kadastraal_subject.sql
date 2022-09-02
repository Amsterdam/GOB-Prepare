SELECT sjt.Identificatie_subject											AS identificatie
--, sjt.nrn_sjt_id															AS nrn_sjt_id											--jr, 2022-08-30: niet nodig
, sjt.Type_subject															AS type_subject
, sjt.Code_Beschikkingsbevoegdheid											AS beschikkingsbevoegdheid_code
, bbd.omschrijving							 								AS beschikkingsbevoegdheid_omschrijving
, sjt.Indicatie_afscherming_gegevens										AS indicatie_afscherming_gegevens						--jr, 2022-08-30: nieuw
, sjt.Heeft_BSN_voor														AS heeft_bsn_voor
, sjt.Code_naam_gebruik														AS naam_gebruik_code
, ank.omschrijving															AS naam_gebruik_omschrijving
, sjt.Code_titel_of_predicaat												AS titel_of_predicaat_code								--jr, 2022-08-30: nieuw
, ctp.omschrijving															AS titel_of_predicaat_omschrijving						--jr, 2022-08-30: nieuw
, sjt.Indicatie_diakriet_niet_toonbaar										AS indicatie_diacriet_niet_toonbaar						--jr, 2022-08-30: nieuw
, sjt.Geslachtsnaam															AS geslachtsnaam
, sjt.Voornamen																AS voornamen
, sjt.Voorvoegsels 															AS voorvoegsels
, sjt.Code_geslacht															AS geslacht_code
, aag.omschrijving															AS geslacht_omschrijving
, sjt.Geboortedatum															AS geboortedatum
, sjt.Geboortedatum_onvolledig												AS geboortedatum_onvolledig								--jr, 2022-08-30: nieuw
, sjt.Geboorteplaats														AS geboorteplaats
, sjt.Code_geboorteland														AS geboorteland_code
, lad.omschrijving															AS geboorteland_omschrijving
, sjt.Code_land_waarnaar_vertrokken											AS land_waarnaar_vertrokken_code
, lav.omschrijving										 					AS land_waarnaar_vertrokken_omschrijving
, CASE
 -- WHEN bon.overlijdensdatum IS NOT NULL THEN bon.overlijdensdatum 													--tijdelijk uit voor analyse
	 WHEN sjt.datum_overlijdenIS NOT NULL THEN sjt.datum_overlijden
END															 				AS datum_overlijden										--jr, 2022-08-30: !checken of dat goed gaat zo!
, sjt.Datum_overlijden_onvolledig											AS datum_overlijden_onvolledig							--jr, 2022-08-30: nieuw
, sjt.Indicatie_overleden													AS indicatie_overleden 
, sjt.Geslachtsnaam_partner													AS geslachtsnaam_partner
, sjt.Voornamen_partner														AS voornamen_partner
, sjt.Voorvoegsel_partner													AS voorvoegsels_partner
, sjt.Statutaire_naam 														AS statutaire_naam
, sjt.Statutaire_zetel														AS statutaire_zetel
, sjt.Code_rechtsvorm 														AS rechtsvorm_code
, rvm.omschrijving															AS rechtsvorm_omschrijving
, sjt.Heeft_RSIN_voor														AS heeft_rsin_voor
, sjt.Heeft_KvKnummer_voor													AS heeft_kvknummer_voor
	 --**woonadres**	
--, sjt.Woonlocatie_identificatie																									--jr, 2022-08-30: nieuw, maar hoeven we volgens mij niet te tonen
, sjt.Woonlocatie_type														AS woonlocatie_type										--jr, 2022-08-30: nieuw
, obd.bag_identificatie								 						AS woonadres_adresseerbaar_object
, obd.openbareruimtenaam													AS woonadres_openbare_ruimtenaam
, obd.huisnummer															AS woonadres_huisnummer
, obd.huisletter															AS woonadres_huisletter
, obd.huisnummertoevoeging													AS woonadres_huisnummertoevoeging
, obd.postcode																AS woonadres_postcode
, obd.woonplaatsnaam 														AS woonadres_woonplaatsnaam		
, obd.woonplaatsnaam_afwijkend												AS woonadres_woonplaatsnaam_afwijkend					--jr, 2022-08-30: nieuw
, obu.adres																	AS woonadres_buitenland_adres
, obu.woonplaats															AS woonadres_buitenland_woonplaats
, obu.regio																	AS woonadres_buitenland_regio
, obu.land_code																AS woonadres_buitenland_land_code
, lbu.omschrijving															AS woonadres_buitenland_land_naam
	 --**postadres*			
--, sjt.Postlocatie_identificatie																									--jr, 2022-08-30: nieuw, maar hoeven we volgens mij niet te tonen
, sjt.Postlocatie_type														AS postlocatie_type										--jr, 2022-08-30: nieuw
, pbl.postbusnummer															AS postadres_postbusnummer 
, pbl.postcode																AS postadres_postbus_postcode
, pbl.woonplaatsnaam														AS postadres_postbus_woonplaatsnaam
, pad.bag_identificatie														AS postadres_adresseerbaar_object						--jr, 2022-08-30: nieuw
, pad.openbareruimtenaam													AS postadres_openbare_ruimtenaam
, pad.huisnummer															AS postadres_huisnummer
, pad.huisletter															AS postadres_huisletter
, pad.huisnummertoevoeging													AS postadres_huisnummertoevoeging
, pad.postcode																AS postadres_postcode
, pad.woonplaatsnaam 														AS postadres_woonplaatsnaam		
, pad.woonplaatsnaam_afwijkend												AS postadres_woonplaatsnaam_afwijkend					--jr, 2022-08-30: nieuw
, pau.adres																	AS postadres_buitenland_adres
, pau.woonplaats															AS postadres_buitenland_woonplaats
, pau.regio																	AS postadres_buitenland_regio
, pau.land_code																AS postadres_buitenland_land_code
, lbu.omschrijving															AS postadres_buitenland_land_naam
--, bsd.toestandsdatum														AS toestandsdatum										--tijdelijk uit voor analyse
--, ede.expiration_date														AS Datum_actueel_tot									--tijdelijk uit voor analyse
FROM
			((SELECT id									AS nrn_sjt_id
			,		identificatie						AS Identificatie_subject
			,		 'NATUURLIJK PERSOON'				AS Type_subject
			,		beschikkingsbevoegdheid_code		AS Code_Beschikkingsbevoegdheid
			,		ind_diakriet_niet_toonbaar			AS Indicatie_diakriet_niet_toonbaar
			,		postlocatie_identificatie			AS Postlocatie_identificatie
			,		postlocatietype						AS Postlocatie_type
			,		woonlocatie_identificatie			AS Woonlocatie_identificatie
			,		woonlocatietype						AS Woonlocatie_type
			,		ind_overleden						AS Indicatie_overleden
			,		ind_afscherming_gegevens			AS Indicatie_afscherming_gegevens
			,		bsn									AS Heeft_BSN_voor
			,		titel_of_predicaat_code				AS Code_titel_of_predicaat
			,		aanduiding_naamgebruik_code			AS Code_naam_gebruik
			,		land_waarnaar_vertrokken_code		AS Code_land_waarnaar_vertrokken
			,		geslachtsnaam						AS Geslachtsnaam
			,		voornamen							AS Voornamen
			,		voorvoegsel							AS Voorvoegsels 
			,		geslachtsaanduiding_code			AS Code_geslacht
			,		geboortedatum		 				AS Geboortedatum
			,		geboortedatum_onv					AS Geboortedatum_onvolledig
			,		geboorteplaats						AS Geboorteplaats
			,		geboorteland_code					AS Code_geboorteland
			,		overlijdensdatum					AS Datum_overlijden
			,		overlijdensdatum_onv				AS Datum_overlijden_onvolledig
			,		partner_geslachtsnaam				AS Geslachtsnaam_partner	
			,		partner_voornamen				 	AS Voornamen_partner
			,		partner_voorvoegsel					AS Voorvoegsel_partner
			,		NULL 								AS Statutaire_naam 
			,		NULL 								AS Statutaire_zetel
			,		NULL 								AS Code_rechtsvorm 
			,		NULL 								AS Heeft_RSIN_voor
			,		NULL 								AS Heeft_KvKnummer_voor
			FROM brk2.natuurlijk_persoon)
			UNION
			(SELECT id									AS nrn_sjt_id
			,		identificatie						AS Identificatie_subject
			,		 'NATUURLIJK PERSOON'				AS Type_subject
			,		beschikkingsbevoegdheid_code		AS Code_Beschikkingsbevoegdheid
			,		ind_diakriet_niet_toonbaar			AS Indicatie_diakriet_niet_toonbaar
			,		postlocatie_identificatie			AS Postlocatie_identificatie	
			,		postlocatietype						AS Postlocatie_type
			,		woonlocatie_identificatie			AS Woonlocatie_identificatie
			,		woonlocatietype						AS Woonlocatie_type
			,		NULL								AS Indicatie_overleden
			,		NULL								AS Indicatie_afscherming_gegevens
			,		NULL								AS Heeft_BSN_voor
			,		NULL								AS Code_titel_of_predicaat
			,		NULL								AS Code_naam_gebruik
			,		NULL								AS Code_land_waarnaar_vertrokken
			,		NULL								AS Geslachtsnaam
			,		NULL								AS Voornamen
			,		NULL								AS Voorvoegsels 
			,		NULL								AS Code_geslacht
			,		NULL								AS Geboortedatum
			,		NULL								AS Geboortedatum_onvolledig
			,		NULL								AS Geboorteplaats
			,		NULL								AS Code_geboorteland
			,		NULL								AS Datum_overlijden
			,		NULL								AS Datum_overlijden_onvolledig
			,		NULL								AS Geslachtsnaam_partner	
			,		NULL								AS Voornamen_partner
			,		NULL								AS Voorvoegsel_partner
			,		statutairenaam						AS Statutaire_naam 
			,		statutairezetel						AS Statutaire_zetel
			,		rechtsvorm_code						AS Code_rechtsvorm 
			,		rsin								AS Heeft_RSIN_voor
			,		kvknummer							AS Heeft_KvKnummer_voor
			FROM brk2.niet_natuurlijk_persoon)) sjt
LEFT	JOIN brk2.objectlocatie_binnenland		obd	ON (sjt.woonlocatie_identificatie = obd.identificatie)
LEFT	JOIN brk2.objectlocatie_buitenland		obu	ON (sjt.woonlocatie_identificatie = obu.identificatie)	
LEFT	JOIN brk2.objectlocatie_binnenland		pad	ON (sjt.postlocatie_identificatie = pad.identificatie)
LEFT	JOIN brk2.objectlocatie_buitenland		pau	ON (sjt.postlocatie_identificatie = pau.identificatie)	
LEFT	JOIN brk2.postbus_locatie	 			pbl	ON (sjt.postlocatie_identificatie = pbl.identificatie)	
-- CODETABELLEN
LEFT	JOIN brk2.c_beschikkingsbevoegdheid		bbd	ON (sjt.Code_beschikkingsbevoegdheid = bbd.code)
LEFT	JOIN brk2.c_aanduidinggeslacht			aag	ON (sjt.Code_geslacht = aag.code)
LEFT	JOIN brk2.c_aanduidingnaamgebruik		ank	ON (sjt.Code_naam_gebruik = ank.code)
LEFT	JOIN brk2.c_aanduidinggeslacht			agt	ON (sjt.Code_geslacht = agt.code)
LEFT	JOIN brk2.c_land						lad	ON (sjt.Code_geboorteland = lad.code)
LEFT	JOIN brk2.c_land						lav	ON (sjt.Code_land_waarnaar_vertrokken = lav.code)
LEFT	JOIN brk2.c_rechtsvorm					rvm	ON (sjt.Code_rechtsvorm = rvm.code)
LEFT	JOIN brk2.c_titelofpredicaat			ctp	ON (sjt.Code_titel_of_predicaat = ctp.code)
LEFT	JOIN brk2.c_land						lbu	ON (obu.land_code = lbu.code)
LEFT	JOIN brk2.c_land						pbu	ON (pau.land_code = pbu.code)
-- tijdelijk uit voor analyse 			LEFT	JOIN brk_prep.subject_expiration_date ede	ON (sjt.identificatie=ede.subject_id)	--jr, 31-08-2022, deze moeten we nog nakijken, weet niet of dit 1 op 1 mee kan zo
-- tijdelijk uit voor analyse			LEFT	JOIN brk_prep.bsn_overleden			bon	ON (sjt.bsn=bon.bsn)						--jr, 31-08-2022, denk dat dit wel zo blijft
-- tijdelijk uit voor analyse			JOIN	brk.bestand bsd								ON (1 = 1)								--jr, 31-08-2022, denk dat dit wel zo blijft