SELECT kot.identificatie                    AS brk_kot_id             
     , kot.id                               AS nrn_kot_id             
     , kot.id                               AS source_id              
     , kot.volgnummer                       AS nrn_kot_volgnr         
    , json_build_object(												
       'code', kot.kadastralegemeente_code,						  
       'omschrijving', kge.omschrijving							  
      )                                       AS kad_gemeente
    , json_build_object(
        'code', LPAD(brg.cbscode::text, 4, '0'),					  
        'omschrijving', brg.bgmnaam									  
      )                                       AS brg_gemeente
     , json_build_object(
        'code', kot.akrkadastralegemeentecode_code,					  
        'omschrijving', kot.akrkadastralegemeentecode   --uit kot zelf nu, niet meer via hulptabel							  
      )                                       AS kad_gemeentecode
     , kot.akrkadastralegemeentecode || kot.sectie || LPAD(kot.perceelnummer::text, 5, '0') || kot.index_letter ||    
       LPAD(kot.index_nummer::text, 4, '0') AS kadastrale_aanduiding	
     , kot.sectie                           AS sectie				   		
     , kot.perceelnummer                    AS perceelnummer           		
     , kot.index_letter                     AS index_letter            		
     , kot.index_nummer                     AS index_nummer			   		
     , kot.soortgrootte_code                AS soortgrootte_code	   		
     , sge.omschrijving                     AS soortgrootte_oms		   		
     , kot.kadgrootte                       AS kadgrootte			   		
     , kot.koop_som                         AS koopsom				   		
     , kot.koop_valuta_code                 AS koopsom_valuta		   		
     , kot.koopjaar                         AS koopjaar				   		
     , kot.indicatiemeerobjecten            AS indicatie_meer_objecten 		
     , kot.cultuurcodeonbebouwd_code        AS cultuurcodeonbebouwd_code 	
     , cod.omschrijving                     AS cultuurcodeonbebouwd_oms
     , kot.cultuurcodebebouwd_code          AS cultuurcodebebouwd_code  --nieuw, ipv json object
	 , ccb.omschrijving                     AS cultuurcodeonbebouwd_oms  --nieuw, ipv json object
     , kot.status_code                      AS status_code																			
--   --, coalesce(vkg.vkgrens, 'N')           AS ind_voorlopige_kadgrens 			--wordt vanaf nu (waarschijnlijk) afgeleid uit soortgrootte, zie commentaar bij de join. in eerste instantie dus niet meer leveren										
     , kok.omschrijving                     AS inonderzoek      --nieuw          																
     , kot.referentie						AS referentie   --nieuw
	 , kot.oudst_digitaal_bekend			AS oudst_digitaal_bekend --nieuw
	 , kot.mutatie_id						AS mutatie_id --nieuw
	 , kot.meettarief_verschuldigd          AS meettarief_verschuldigd --nieuw
	 , kot.toelichting bewaarder			AS toelichting_bewaarder --nieuw
	 , kot.tijdstip_ontstaan_object			AS tijdstip_ontstaan_object --nieuw
	 , kot.hoofdsplitsing_identificatie		AS hoofdsplitsing_identificatie --nieuw
	 , kot.afwijking_lijst_rechthebbenden	AS afwijking_lijst_rechthebbenden --nieuw
     , kot.toestandsdatum                   AS toestandsdatum  		 		
     , kot.creation                         AS creation 			 		
     , kot.modification                     AS modification          		
     , CASE		
           WHEN kot.modification IS NOT NULL						 		
               THEN kot.modification								 		
           ELSE		
               (CASE kot.status_code								 		
                    WHEN 'H' THEN kot.creation 			             		
                    ELSE NULL END) END      AS expiration_date		
     , kot.modification                     as einddatum             		
     --     Wanneer het een A-perceel betreft		
     --     DAN geometrie afleiden uit of meer grondpercelen
     --      in GOB later in het proces--
     , CASE kot.index_letter            							  		
           WHEN 'G' THEN
               kot.geometrie												
           ELSE
               NULL
    END                                     AS geometrie
     , prc.rotatie                          AS perceelnummer_rotatie		
     , prc.verschuiving_x                   AS perceelnummer_verschuiving_x 
     , prc.verschuiving_y                   AS perceelnummer_verschuiving_y 
     , prc.geometrie                        AS perceelnummer_geometrie		
     , bij.geometrie                        AS bijpijling_geometrie			
     , adr.adressen                         AS adressen							        
     , brg.bgmnaam                          AS brg_gemeente_oms    	         			
--
FROM kadastraal_object kot
        LEFT JOIN c_kadastralegemeente kge
                   ON (kadastralegemeente_code = kge.code)
        LEFT JOIN brk.import_cultuur_onbebouwd cod 
                   ON (kot.cultuurcodeonbebouwd_code = cod.code)
        LEFT JOIN c_cultuurcodeonbebouwd ccb
		           ON (kot.cultuurcodebebouwd_code = ccb.code)
        LEFT JOIN c_soortgrootte sge
                   ON (kot.soortgrootte_code = sge.code)
        LEFT JOIN kadastraal_object_onderzoek koo
                   ON (kot.id = koo.kadastraalobject_id AND             															
                       kot.volgnummer = koo.kadastraalobject_volgnummer)															
		LEFT JOIN inonderzoek io																								
				   ON  koo.onderzoek_identificatie=io.identificatie
        LEFT JOIN c_authentiekgegeven 	kok
				   ON io.authentiekgegeven_code=kok.code																			
--Cultuurcode bebouwd, kunnen er meer per kadastraal object zijn																	
     /*  LEFT JOIN (SELECT kas.kot_id                                                               AS nrn_kot_id
                         , kas.kot_volgnr                                                           AS nrn_kot_volgnr
                         , array_to_json(array_agg(json_build_object( -- POSTGRES Changed to JSON
                                                           'code', kas.cult_beb_code,
                                                           'omschrijving', kas.cult_beb
                                                       ) ORDER BY kas.cult_beb_code, kas.cult_beb)) as cultuurbebouwd
                    FROM (SELECT kasi.kadastraalobject_id         AS kot_id
                               , kasi.kadastraalobject_volgnummer AS kot_volgnr
                               , cbd.omschrijving                 AS cult_beb
                               , cbd.code                         AS cult_beb_code
                          FROM brk.kadastraal_adres kasi
                                   JOIN brk.import_cultuur_bebouwd cbd
                                        ON (kasi.cultuurbebouwd_code = cbd.code)
                          WHERE kasi.cultuurbebouwd_code IS NOT NULL
                          GROUP BY kasi.kadastraalobject_id
                                 , kasi.kadastraalobject_volgnummer
                                 , cbd.omschrijving
                                 , cbd.code) kas
                    GROUP BY kas.kot_id
                           , kas.kot_volgnr) ccb
                   ON (kot.id = ccb.nrn_kot_id AND kot.volgnummer = ccb.nrn_kot_volgnr)										*/  	--tot hier is overbodig nu
				   
				   
    -- Voorlopige_grens: indicatie voorlopige kadastrale grens wordt afgeleid uit aantekening en aantekening_kadastraalobject      
																																	--Deze hele constructie lijkt overbodig te worden. In de BRK2 documentatie staat namelijk:									
																																	--De aantekeningen 271 ‘Voorlopige kadastrale grens en oppervlakte’ en 270 ‘Administratieve voorlopige									
																																	--(kadastrale) grens’ worden niet meer geleverd. Deze informatie kan al sinds de livegang van Koers 									
																																	--(11-10-2018) worden afgeleid uit de soortGrootte.	Vraag is nog wel of er nog oude aantekeningen 270 en 271 zijn die nog geldig zijn.									
-- 271 Voorlopige kadastrale grens en oppervlakte                                           										--dit moet nagevraagd worden bij vicrea en kadaster					
																																	--ook moet bekeken worden welke waarden in c_soortgrootte we als 'voorlopig' beschouwen			
       /*  LEFT JOIN (SELECT akt.kadastraalobject_id         AS nrn_kot_id
                         , akt.kadastraalobject_volgnummer AS nrn_kot_volgnr
                         , 'J'                             AS vkgrens -- Replace 'Voorlopige grens' with 'J'
                    FROM brk.aantekening_kadastraalobject akt
                       , brk.aantekening atg
                    WHERE atg.id = akt.aantekening_id
                      AND (date_trunc('day', einddatum) <= date_trunc('day', NOW()) or 
                           einddatum IS NULL)  -- POSTGRES: replaced trunc(einddatum) with date_trunc('day', einddatum) 
												--and trunc(SYSDATE) with date_trun('day', NOW())
                      AND aardaantekening_code IN ('270', '271')
                      --ontdubbel voorlopige grenzen per kadastraal object en cyclus (diva 2.30.8)							
                      AND (akt.kadastraalobject_id, kadastraalobject_volgnummer,
                           aantekening_id) IN
                          (SELECT akt.kadastraalobject_id
                                , akt.kadastraalobject_volgnummer
                                , MAX(aantekening_id)
                           FROM brk.aantekening_kadastraalobject akt                                                        		--kadastraal_object_aantekening
                              , brk.aantekening atg		
                           WHERE atg.id = akt.aantekening_id																		--deze join lukt in de testdata niet omdat in aantekening-id nog oude id's zitten, 
																																	--zonder 'imkad' er in. in akt_id zitten alleen id's mét 'imkad'
																																	--als dat structureel blijkt dan moeten we een of andere concat oit gebruiken
                             AND (date_trunc('day', einddatum) <= date_trunc('day', NOW()) OR -- POSTGRES: replaced trunc(einddatum) 
																							  --with date_trunc('day', einddatum) 
																							  --and trunc(SYSDATE) with date_trun('day', NOW())
                                  einddatum IS NULL)
                             AND aardaantekening_code IN
                                 ('270', '271')
                           GROUP BY akt.kadastraalobject_id
                                  , akt.kadastraalobject_volgnummer)) vkg
                   ON (kot.id = vkg.nrn_kot_id AND kot.volgnummer = vkg.nrn_kot_volgnr)			*/									--Dit blok is waarschijnlijk overbodig, zie hierboven
        LEFT JOIN kadastraal_object_percnummer prc																		
                   ON (kot.id = prc.kadastraalobject_id AND kot.volgnummer = prc.kadastraalobject_volgnummer)
        LEFT JOIN kadastraal_object_bijpijling bij																		
                   ON (kot.id = bij.kadastraalobject_id AND kot.volgnummer = bij.kadastraalobject_volgnummer);
        LEFT JOIN brk.baghulptabel adr
                   ON adr.kadastraalobject_id = kot.id and adr.kadastraalobject_volgnummer = kot.volgnummer
        LEFT JOIN (SELECT cbscode, bgmnaam, kadgemnaam
                   FROM brk.import_burgerlijke_gemeentes
                   GROUP BY cbscode, bgmnaam, kadgemnaam) brg
                   ON (kge.omschrijving = brg.kadgemnaam);
				   
				   
				   
				   --nog te doen: 
				   --kadastraal_object_landrente,  --weet niet wat dit is, mist in documentatie
				   --kadastraal_object_mandeligheid,  --is nu helemaal leeg dus moeilijk te duiden; misschien een aparte objectklasse van maken?? 
				   --kadastraal_object_ontstaan_uit --is een aparte objectklasse; filiatie, hiervoor moet een aparte import komen, zie voorstel in 5a_voorstel_import_filiatie.sql
				   --kadastraal_object.herverkaveling en de joins daarbij --is nog de vraag of we daar wat mee willen en of het voorkomt binnen ons gebied
				   