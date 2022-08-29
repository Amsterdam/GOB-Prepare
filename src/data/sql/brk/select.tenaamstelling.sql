-- Query assumes brk_prep.zakelijk_recht is fully populated and ready.
-- Volgnummer, begindatum and einddatum are taken from ZRT; ZRT in turn gets these from KOT: this is how it works in
-- the source database.
-- We get these attributes from the brk_prep.zakelijk_recht table instead of from the kadastraalobject table from the
-- brk schema, because the kadastraal object references aren't populated on all ZRT objects in the brk schema.
SELECT t.identificatie              									AS brk_tng_id				            --identificatie
--,      t.id                   										AS nrn_tng_id 				            --neuron_id
,      t.tennamevan_identificatie      									AS van_brk_subject_id 		            --van_brk_kadastraal_subject								--jr, 2022-08-24: ipv  t.van_persoon_identificatie
,      t.tennamevantype                									AS tennamevan_type_code 	            --tennamevan_type_code										--jr, 2022-08-24 nieuw, geeft aan welk type persoon het is
--,      sjt.id                    										AS van_brk_nrn_subject_id	            --__van_brk_nrn_subject_id									--jr, 2022-08-24: lijkt mij overbodig, kan ook met landelijke id?, anders via natuurlijkpersoon en nietnatuurlijkpersoon
,      t.aandeel_teller              									AS aandeel_teller							            
,      t.aandeel_noemer              									AS aandeel_noemer							            
,      ga.teller			           									AS geldt_voor_teller  							            										--jr, 2022-08-26: op basis van ander kenmerk
,      ga.noemer          												AS geldt_voor_noemer  										            							--jr, 2022-08-26: op basis van ander kenmerk
,      t.burgerlijkestaat_code         									AS burgerlijkestaat_code	            --burgerlijke_staat_ten_tijde_van_verkrijging_code
,      b.omschrijving               									AS burgerlijkestaat_oms		            --burgerlijke_staat_ten_tijde_van_verkrijging_omschrijving
,      t.betrokkenpartner_identificatie									AS betrokkenpartner_brk_subject_id		--betrokken_partner_brk_subject_identificatie				--jr, 2022-08-26: nieuw, verwijst naar een natuurlijkpersoon
,      t.verkregen_namens_code         									AS verkrnamens_code 		            --verkregen_namens_samenwerkingsverband_code
,      s.omschrijving               									AS verkrnamens_oms 			            --verkregen_namens_samenwerkingsverband_omschrijving
,      t.betrokkensvb_identificatie										AS betrokkensvb_brk_subject_id			--betrokken_samenwerkingsverband_brk_subject_identificatie	--jr, 2022-08-26: nieuw, verwijst naar een nietnatuurlijkpersoon
,	   t.betrokkengorswas_identificatie									AS betrokken_gors_aanwas_brk_subject_id --betrokken_gorzen_en_aanwassen_brk_subject_identificatie	--jr, 2022-08-26: nieuw, verwijst naar een nietnatuurlijkpersoon
,      ag.omschrijving          										AS inonderzoek				            --in_onderzoek												--jr, 2022-08-24: komt nu ergens anders vandaan en beschrijft wat er in onderzoek is
,      zrt.identificatie               									AS van_brk_zakelijkrecht_id		        --van_brk_zakelijk_recht_identificatie						
--,      t.vanrecht_id               									AS van_brk_nrn_zakelijkrecht_id         --__van_brk_zakelijk_recht_neuron_id						--jr, 2022-08-24 ipv  t.van_id
,      g.stukdeel_identificatie        									AS gebaseerdop_brk_stukdeel_id	        --is_gebaseerd_op_brk_stukdeel_identificatie							
,      zrt.toestandsdatum              									AS toestandsdatum					            
,      zrt.rust_op_kadastraalobj_volgnr									AS volgnummer								            
,      zrt.zrt_begindatum              									AS begindatum				            --begin_geldigheid									
,      LEAST(zrt.expiration_date, atg.einddatum, atg.einddatum_recht) 	AS einddatum				            --eind_geldigheid											--jr, 2022-08-26: blijft; einddatum_recht toegevoegd. Nog checken of expiration_date hier nog zo heet
,      LEAST(zrt.expiration_date, atg.einddatum, atg.einddatum_recht) 	AS datum_actueel_tot					            												--jr, 2022-08-26: nieuw	, andere naam voor expiration_date
,      zrt.creation                         							AS creation					            --__creation
,      zrt.modification                     							AS modification				            --__modification
FROM BRK2.TENAAMSTELLING t
LEFT JOIN BRK2.TENAAMSTELLING_ISGEBASEERDOP g    ON t.id=g.tenaamstelling_id
LEFT JOIN BRK2.C_SAMENWERKINGSVERBAND s          ON t.verkregen_namens_code=s.code
LEFT JOIN BRK2.C_BURGERLIJKESTAAT b              ON t.burgerlijkestaat_code=b.code
--LEFT JOIN BRK2.SUBJECT sjt                       ON t.van_persoon_identificatie=sjt.identificatie
LEFT JOIN brk_prep.zakelijk_recht zrt            ON t.van_id=zrt.id
LEFT JOIN BRK2.TENAAMSTELLING_ONDERZOEK o        ON t.id=o.tenaamstelling_id
LEFT JOIN BRK2.INONDERZOEK io 					 ON o.onderzoek_identificatie=io.identificatie
LEFT JOIN BRK2.C_authentiekgegeven  ag           ON io.authentiekgegeven_code=ag.code
LEFT JOIN BRK2.gezamenlijk_aandeel ga            on t.geldtvoordeel_identificatie=ga.identificatie
LEFT JOIN (
    SELECT
        art.tenaamstelling_identificatie,
        atg.einddatum
    FROM brk.aantekeningrecht art
    LEFT JOIN brk.aantekening atg ON atg.id = art.aantekening_id
    -- aardaantekening_code 21 is Einddatum recht 
	--jr, 2022-08-24: er staat bij aantekening in de documentatie: 'Een einddatum op een zakelijk recht (via tenaamstelling) wordt nu in een apart attribuut geleverd.
	--Voorheen werd dit geleverd in het attribuut einddatum.Tenaamstellingen van een eindig recht worden niet automatisch beëindigd. 
	--Wel krijgt zo’n tenaamstelling de aantekening dat het recht eindig is en op een bepaalde datum zal eindigen of geëindigd is'
	--ik zie dat bij 21 altijd óf einddatum óf einddatum_recht gevuld is. Het lijkt alsof einddatum in de toekomst niet meer gebruikt gaat worden, maar we moeten ze voor nu beiden gebruiken
    WHERE art.id IN (
        SELECT max(art.id)
        FROM brk.aantekening atg
        LEFT JOIN brk.aantekeningrecht art ON art.aantekening_id = atg.id
        where atg.aardaantekening_code = '21' group by art.tenaamstelling_identificatie
    )
) atg ON atg.tenaamstelling_identificatie = t.identificatie
JOIN   brk.bestand bsd                          ON (1 = 1);