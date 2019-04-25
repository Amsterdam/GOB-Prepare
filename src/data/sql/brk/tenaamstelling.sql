SELECT t.identificatie              	AS brk_tng_id
,      t.id                   			AS nrn_tng_id
,      t.van_persoon_identificatie      AS van_subject_id
,      sjt.id                    		AS van_nrn_subject_id
,      t.aandeel_teller              	AS aandeel_teller
,      t.aandeel_noemer              	AS aandeel_noemer
,      t.geldt_voor_teller            	AS geldt_voor_teller
,      t.geldt_voor_noemer            	AS geldt_voor_noemer
--einddatum -- bepalen in functie
--actueel -- bepalen in functie
,      t.burgerlijkestaat_code          AS burgerlijkestaat_code
,      b.omschrijving               	AS burgerlijkestaat_oms
,      t.verkregen_namens_code          AS verkrnamens_code
,      s.omschrijving               	AS verkrnamens_oms
,      o.omschrijving          			AS inonderzoek
,      zrt.identificatie                AS van_zakelijkrecht_id
,      t.van_id               			AS van_nrn_zakelijkrecht_id
,      g.stukdeel_identificatie         AS gebaseerdop_stukdeel_id
,      bsd.brk_bsd_toestandsdatum       AS toestandsdatum
,	   CASE
             WHEN kot.modification IS NOT NULL
                 THEN kot.modification
             ELSE
                 (CASE kot.status_code
                  WHEN 'H' THEN kot.creation
                     ELSE NULL END)  END AS einddatum
FROM BRK.TENAAMSTELLING t
LEFT JOIN BRK.TENAAMSTELLING_ISGEBASEERDOP g    ON t.id=g.tenaamstelling_id
LEFT JOIN BRK.C_SAMENWERKINGSVERBAND s          ON t.verkregen_namens_code=s.code
LEFT JOIN BRK.C_BURGERLIJKESTAAT b              ON t.burgerlijkestaat_code=b.code
LEFT JOIN BRK.SUBJECT sjt                       ON t.van_persoon_identificatie=sjt.identificatie
LEFT JOIN BRK.ZAKELIJKRECHT zrt                 ON t.van_id=zrt.id
LEFT JOIN BRK.TENAAMSTELLING_ONDERZOEK o        ON t.id=o.tenaamstelling_id
LEFT JOIN BRK.kadastraal_object kot             ON kot.id=zrt.rust_op_kadastraalobject_id AND kot.volgnummer=zrt.rust_op_kadastraalobj_volgnr
JOIN   brk.bestand bsd                          ON (1 = 1);