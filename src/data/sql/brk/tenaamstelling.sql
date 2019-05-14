-- Query assumes brk_prep.zakelijk_recht is fully populated and ready.
-- Volgnummer, begindatum and einddatum are taken from ZRT; ZRT in turn gets these from KOT: this is how it works in
-- the source database.
-- We get these attributes from the brk_prep.zakelijk_recht table instead of from the kadastraalobject table from the
-- brk schema, because the kadastraal object references aren't populated on all ZRT objects in the brk schema.
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
,      zrt.rust_op_kadastraalobj_volgnr AS volgnummer
,      zrt.zrt_begindatum               AS begindatum
,      zrt.zrt_einddatum                AS einddatum
FROM BRK.TENAAMSTELLING t
LEFT JOIN BRK.TENAAMSTELLING_ISGEBASEERDOP g    ON t.id=g.tenaamstelling_id
LEFT JOIN BRK.C_SAMENWERKINGSVERBAND s          ON t.verkregen_namens_code=s.code
LEFT JOIN BRK.C_BURGERLIJKESTAAT b              ON t.burgerlijkestaat_code=b.code
LEFT JOIN BRK.SUBJECT sjt                       ON t.van_persoon_identificatie=sjt.identificatie
LEFT JOIN brk_prep.zakelijk_recht zrt                 ON t.van_id=zrt.id
LEFT JOIN BRK.TENAAMSTELLING_ONDERZOEK o        ON t.id=o.tenaamstelling_id
JOIN   brk.bestand bsd                          ON (1 = 1);