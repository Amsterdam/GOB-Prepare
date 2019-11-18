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
,      t.burgerlijkestaat_code          AS burgerlijkestaat_code
,      b.omschrijving               	AS burgerlijkestaat_oms
,      t.verkregen_namens_code          AS verkrnamens_code
,      s.omschrijving               	AS verkrnamens_oms
,      o.omschrijving          			AS inonderzoek
,      zrt.identificatie                AS van_zakelijkrecht_id
,      t.van_id               			AS van_nrn_zakelijkrecht_id
,      g.stukdeel_identificatie         AS gebaseerdop_stukdeel_id
,      zrt.toestandsdatum               AS toestandsdatum
,      zrt.rust_op_kadastraalobj_volgnr AS volgnummer
,      zrt.zrt_begindatum               AS begindatum
,      least(zrt.expiration_date, atg.einddatum) AS einddatum
FROM BRK.TENAAMSTELLING t
LEFT JOIN BRK.TENAAMSTELLING_ISGEBASEERDOP g    ON t.id=g.tenaamstelling_id
LEFT JOIN BRK.C_SAMENWERKINGSVERBAND s          ON t.verkregen_namens_code=s.code
LEFT JOIN BRK.C_BURGERLIJKESTAAT b              ON t.burgerlijkestaat_code=b.code
LEFT JOIN BRK.SUBJECT sjt                       ON t.van_persoon_identificatie=sjt.identificatie
LEFT JOIN brk_prep.zakelijk_recht zrt                 ON t.van_id=zrt.id
LEFT JOIN BRK.TENAAMSTELLING_ONDERZOEK o        ON t.id=o.tenaamstelling_id
LEFT JOIN (
    SELECT
        art.tenaamstelling_identificatie,
        atg.einddatum
    FROM brk.aantekeningrecht art
    LEFT JOIN brk.aantekening atg ON atg.id = art.aantekening_id
    -- aardaantekening_code 21 is Einddatum recht
    WHERE art.id IN (
        SELECT max(art.id)
        FROM brk.aantekening atg
        LEFT JOIN brk.aantekeningrecht art ON art.aantekening_id = atg.id
        where atg.aardaantekening_code = '21' group by art.tenaamstelling_identificatie
    )
) atg ON atg.tenaamstelling_identificatie = t.identificatie
JOIN   brk.bestand bsd                          ON (1 = 1);