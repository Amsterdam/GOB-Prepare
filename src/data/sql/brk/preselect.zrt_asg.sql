SELECT zrt.identificatie,
       zrt.id,
       zrt.rust_op_kadastraalobj_volgnr,
       zrt_kot.zrt_begindatum           AS zrt_begindatum,
       asg1.id                          AS betrokken_bij_asg_id,
       asg2.id                          AS ontstaan_uit_asg_id,
       asg1.vve                         AS betrokken_bij_ref,
       asg2.vve                         AS ontstaan_uit_ref,
       asg1.id                          AS nrn_asg_id,
       CASE
           WHEN asg2.vve IS NOT NULL
               AND asg1.vve IS NOT NULL THEN
                       asg2.app_rechtsplitstype_code || ',' ||
                       asg1.app_rechtsplitstype_code
           ELSE
               COALESCE(asg2.app_rechtsplitstype_code, asg1.app_rechtsplitstype_code)
           END                          AS asg_app_rechtsplitstype_code, -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs
       CASE
           WHEN asg2.vve IS NOT NULL
               AND asg1.vve IS NOT NULL THEN
               ase2.omschrijving || ',' || ase1.omschrijving
           ELSE
               COALESCE(ase2.omschrijving, ase1.omschrijving)
           END                          AS asg_app_rechtsplitstype_oms,  -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs
       CASE WHEN asg2.id IS NOT NULL THEN 'nog niet beschikbaar' END-- BRK_EXTRACTIE_PCK.geef_einddatum_asg_fnc(tng.id)
                                        AS asg_einddatum,                -- t.b.v. mview met actuele Zakelijk-rechtgegevens
       CASE WHEN asg2.id IS NOT NULL THEN 'nog niet beschikbaar' END -- BRK_EXTRACTIE_PCK.geef_asg_actueel_fnc(tng.id)
                                        AS asg_actueel                   -- t.b.v. mview met actuele Zakelijk-rechtgegevens
FROM brk.zakelijkrecht zrt
         LEFT JOIN brk_prep.zrt_kot zrt_kot
                   ON zrt.id = zrt_kot.id AND zrt.rust_op_kadastraalobj_volgnr = zrt_kot.rust_op_kadastraalobj_volgnr
         LEFT JOIN brk.appartementsrechtsplitsing asg1 ON (asg1.id = zrt.betrokken_bij)
         LEFT JOIN brk.appartementsrechtsplitsing asg2 ON (asg2.id = zrt.ontstaan_uit)
         LEFT JOIN brk.c_appartementsrechtsplitstype ase1 ON (ase1.code = asg1.app_rechtsplitstype_code)
         LEFT JOIN brk.c_appartementsrechtsplitstype ase2 ON (ase2.code = asg2.app_rechtsplitstype_code)
;