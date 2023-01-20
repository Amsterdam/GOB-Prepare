WITH asg_codes(code, omschrijving) AS (VALUES (1, 'HoofdSplitsing'),
                                              (2, 'OnderSplitsing'),
                                              (3, 'SplitsingAfkoopErfpacht'))
SELECT zrt.identificatie,
       zrt.id,
       zrt.rust_op_kadastraalobj_volgnr AS volgnummer,
       zrt_kot.begin_geldigheid         AS begin_geldigheid,
       asg1.id                          AS __betrokken_bij_asg_id,
       asg2.id                          AS __ontstaan_uit_asg_id,
       asg1.vve_identificatie           AS vve_identificatie_betrokken_bij,
       asg2.vve_identificatie           AS vve_identificatie_ontstaan_uit,
       CASE
           WHEN asg2.identificatie IS NOT NULL
               AND asg1.identificatie IS NOT NULL THEN
                       asg2.splitsingstype::varchar || ',' ||
                       asg1.splitsingstype::varchar
           ELSE
               COALESCE(asg2.splitsingstype::varchar, asg1.splitsingstype::varchar)
           END                          AS appartementsrechtsplitsingtype_code,
       CASE
           WHEN asg2.identificatie IS NOT NULL
               AND asg1.identificatie IS NOT NULL THEN
               ase2.omschrijving || ',' || ase1.omschrijving
           ELSE
               COALESCE(ase2.omschrijving, ase1.omschrijving)
           END                          AS appartementsrechtsplitsingtype_omschrijving
FROM brk2.zakelijkrecht zrt
         LEFT JOIN brk2_prep.zrt_kot zrt_kot
                   ON zrt.id = zrt_kot.id AND zrt.rust_op_kadastraalobj_volgnr = zrt_kot.volgnummer
         LEFT JOIN brk2.appartementsrechtsplitsing asg1 ON asg1.identificatie = zrt.isbetrokkenbij_identificatie
         LEFT JOIN brk2.appartementsrechtsplitsing asg2 ON asg2.identificatie = zrt.isontstaanuit_identificatie
         LEFT JOIN asg_codes ase1 ON ase1.code = asg1.splitsingstype
         LEFT JOIN asg_codes ase2 ON ase2.code = asg2.splitsingstype
;