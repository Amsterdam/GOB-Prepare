						SELECT sdl.identificatie                         AS brk_sdl_id
                        , sdl.id                                    AS nrn_sdl_id
                        , sdl.aardstukdeel_code                     AS sdl_aard_stukdeel_code   --jr, 20220923, lijkt vaak NIL te zijn, hoe komt dat? (was in brk1 ook al zo)
                        , asl.omschrijving                          AS sdl_aard_stukdeel_oms
                        , sdl.bedragtransactiesomlevering           AS sdl_koopsom
                        , sdl.bedrtransactiesomlev_vlt_code         AS sdl_koopsom_valuta
                        , stk.identificatie                         AS brk_stk_id
                        , stk.id                                    AS nrn_stk_id
                        , stk.akrportefeuillenr                     AS stk_akr_portefeuillenr
                        , stk.tijdstip_aanbieding                   AS stk_tijdstip_aanbieding
                        , stk.reeks_code                            AS stk_reeks_code
                        , stk.nummer                                AS stk_volgnummer
                        , stk.registercode_code                     AS stk_registercode_code
                        , rce.omschrijving                          AS stk_registercode_oms
                        , stk.soortregister_code                    AS stk_soortregister_code
                        , srr.omschrijving                          AS stk_soortregister_oms
                        , stk.deel                                  AS stk_deel_soort
                        , stk.tekeningingeschreven					AS tekeningingeschreven   --jr, 20220923: nieuw
						, stk.tijdstipondertekening					AS tijdstipondertekening   --jr, 20220923: nieuw
						, stk.toelichtingbewaarder					AS toelichtingbewaarder   --jr, 20220923: nieuw
                        , tng.tng_ids                               AS tng_ids
                        , art.art_ids                               AS art_ids
                        , akt.akt_ids                               AS akt_ids
                        , asg.zrt_ids                               AS zrt_ids
                        , least(tng.min_tng_begindatum, asg.zrt_begindatum) AS begindatum
                        , tng.min_tng_begindatum                    AS min_tng_begindatum
                        , tng.max_tng_begindatum                    AS max_tng_begindatum
                        , CASE
    --     If any einddatum is NULL this stukdeel is still valid. Otherwise get the max
                              WHEN tng.einddatum IS NULL OR asg.zrt_einddatum IS NULL OR akt.expiration_date IS NULL OR
                                   art.expiration_date IS NULL
                                  THEN NULL
                              ELSE greatest(tng.einddatum, asg.zrt_begindatum, akt.expiration_date, art.expiration_date)
        					END                                                         AS expiration_date
                        , bsd.brk_bsd_toestandsdatum                AS toestandsdatum 
                   FROM brk2_jeroen_speeltuin.stukdeel sdl
                            LEFT JOIN brk2_jeroen_speeltuin.stuk stk
                                      ON (sdl.stuk_identificatie= stk.identificatie)
                            LEFT JOIN brk2_jeroen_speeltuin.c_aardstukdeel asl
                                      ON (sdl.aardstukdeel_code = asl.code)
                            LEFT JOIN brk2_jeroen_speeltuin.c_soortregister srr
                                      ON (stk.registercode_code = srr.code)
                            LEFT JOIN brk2_jeroen_speeltuin.c_registercode rce
                                      ON (stk.soortregister_code = rce.code)
          	       		    LEFT JOIN (
                       					SELECT	tip.stukdeel_identificatie
												, min(tng.begindatum) AS min_tng_begindatum
												, max(tng.begindatum) AS max_tng_begindatum
												, CASE
													WHEN sum(CASE WHEN tng.einddatum IS NULL THEN 0 ELSE 1 END) < SUM(1)
															THEN NULL
													ELSE max(tng.einddatum)
													-- NULL if any einddatum is NULL, otherwise MAX of all einddatums
													END                                                                 	AS einddatum
													, array_to_json(array_agg(json_build_object('brk_tng_id', tng.brk_tng_id, 'nrn_tng_id', tng.nrn_tng_id) 
													ORDER BY tng.brk_tng_id, tng.nrn_tng_id)) AS tng_ids
												FROM
													brk2_jeroen_speeltuin.tenaamstelling_isgebaseerdop tip
												LEFT JOIN brk2_jeroen_speeltuin.prep_tenaamstelling tng
												ON
													tng.nrn_tng_id = tip.tenaamstelling_id
												GROUP BY
													tip.stukdeel_identificatie
										) tng ON (sdl.identificatie = tng.stukdeel_identificatie)
                            LEFT JOIN (
                       					SELECT 	stukdeel_identificatie,
                              					array_to_json(array_agg(json_build_object(
                                                'brk_art_id', identificatie,
                                                'nrn_art_id', id
                                                ) ORDER BY identificatie, id)) AS art_ids,
                              			CASE
                                  		WHEN sum(CASE WHEN expiration_date IS NULL THEN 0 ELSE 1 END) < SUM(1)
                                      		THEN NULL
                                  		ELSE max(expiration_date)
                                  		END                                                    					AS expiration_date
                     					FROM (
                                				SELECT	atg.stukdeel_identificatie
														,CASE
															WHEN sum(CASE WHEN art.expiration_date IS NULL THEN 0 ELSE 1 END) < SUM(1)
															THEN NULL
															ELSE max(art.expiration_date)
															END 									AS expiration_date
														,	atg.identificatie
														,	atg.id
												FROM brk2_jeroen_speeltuin.aantekening atg
																															--FROM brk2_jeroen_speeltuin.aantekeningisgebaseerdop aip
																															--LEFT JOIN brk2_jeroen_speeltuin.aantekening atg
																															--	ON 	atg.id = aip.aantekening_id
												JOIN brk2_jeroen_speeltuin.prep_aantekeningrecht art
													-- Only include ART's
													ON	art.nrn_atg_id = atg.id
												GROUP BY atg.stukdeel_identificatie, atg.identificatie, atg.id                            				) q
                       					GROUP BY stukdeel_identificatie
                   					) art ON (sdl.identificatie = art.stukdeel_identificatie)
                            LEFT JOIN (
                       					SELECT stukdeel_identificatie,
                              					array_to_json(array_agg(json_build_object('brk_akt_id', identificatie, 'nrn_akt_id', id) ORDER BY identificatie, id)) AS akt_ids,
                              					CASE
                                  				WHEN sum(CASE WHEN expiration_date IS NULL THEN 0 ELSE 1 END) < SUM(1)
                                      			THEN NULL
                                  				ELSE max(expiration_date)
                                  				END                        			                            AS expiration_date
                       					FROM (
                                				SELECT atg.stukdeel_identificatie,
														CASE
														WHEN sum(CASE WHEN akt.expiration_date IS NULL THEN 0 ELSE 1 END) < SUM(1)
															THEN NULL
														ELSE max(akt.expiration_date)
														END 													AS expiration_date,
														atg.identificatie,
														atg.id
                                				FROM brk2_jeroen_speeltuin.aantekening atg
                                       					--FROM brk2_jeroen_speeltuin.aantekeningisgebaseerdop aip
                                         				--LEFT JOIN brk2_jeroen_speeltuin.aantekening atg
                                                		--	ON atg.id = aip.aantekening_id
                                         		JOIN brk2_jeroen_speeltuin.prep_aantekeningkadastraalobject akt -- Only include AKT's
                                              		ON akt.nrn_atg_id = atg.id
                                				GROUP BY atg.stukdeel_identificatie, atg.identificatie, atg.id
                            				) q
                       					GROUP BY stukdeel_identificatie
                   					) akt ON (sdl.identificatie = akt.stukdeel_identificatie)
                            LEFT JOIN (
                      					SELECT stukdeel_identificatie,
                             	 				min(zrt_begindatum)                                    	AS zrt_begindatum,
                              					CASE
                                  				-- NULL if one of the zrt_einddatums is NULL, otherwise max
                                  				WHEN SUM(CASE WHEN zrt_einddatum IS NULL THEN 0 ELSE 1 END) < SUM(1)
                                      				THEN NULL
                                  				ELSE max(zrt_einddatum)
                                  				END                                               		AS zrt_einddatum,
                              					array_to_json(array_agg(json_build_object('brk_zrt_id', identificatie) ORDER BY identificatie)) AS zrt_ids
                       					FROM (
                                				SELECT asg.stukdeel_identificatie,
                                       			min(zrt.zrt_begindatum) 								AS zrt_begindatum,
                                       			CASE
                                           		-- NULL if one of the zrt_einddatums is NULL, otherwise max
                                           		WHEN SUM(CASE WHEN zrt.zrt_einddatum IS NULL THEN 0 ELSE 1 END) < SUM(1)
                                               		THEN NULL
                                           		ELSE max(zrt.zrt_einddatum)
                                           		END    										            AS zrt_einddatum,
                                		   		zrt.identificatie
                                				FROM brk2_jeroen_speeltuin.appartementsrechtsplitsing asg
														--FROM brk2_jeroen_speeltuin.appartementsrechtspl_stukdeel arl
														--LEFT JOIN brk2_jeroen_speeltuin.appartementsrechtsplitsing asg
														--   ON (arl.appartementsrechtsplitsing_id = asg.id)
                                         		LEFT JOIN brk2_jeroen_speeltuin.prep_zakelijk_recht zrt
                                                   ON (zrt.ontstaan_uit_asg_id = asg.id)
                                				GROUP BY asg.stukdeel_identificatie, zrt.identificatie

                            				) q
                       					GROUP BY stukdeel_identificatie
                   					) asg ON (asg.stukdeel_identificatie = sdl.identificatie)
                            JOIN brk2_jeroen_speeltuin.bestand bsd ON (1 = 1)
    -- Exclude NL.KAD.Stukdeel.33029100 since it has 287.000 relations
    WHERE sdl.identificatie <> 'NL.IMKAD.Stukdeel.33029100'                                                       