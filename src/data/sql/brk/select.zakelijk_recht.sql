SELECT zrt.id
      ,zrt.identificatie
      ,zrt.aardzakelijkrecht_code
      ,a.omschrijving AS aardzakelijkrecht_oms
      ,CASE aardzakelijkrecht_code --bron:http://www.kadaster.nl/schemas/waardelijsten/AardZakelijkRecht/AardZakelijkRecht.gc
             WHEN '1' THEN
              'BK'
             WHEN '2' THEN
              'VE'
             WHEN '3' THEN
              'EP'
             WHEN '4' THEN
              'GB'
             WHEN '5' THEN
              'GR'
             WHEN '7' THEN
              'OS'
             WHEN '9' THEN
              'OVR'
             WHEN '10' THEN
              'BP'
             WHEN '11' THEN
              'SM'
             WHEN '12' THEN
              'VG'
             WHEN '13' THEN
              'EO'
             WHEN '14' THEN
              'OL'
             WHEN '18' THEN
              'OV'
             WHEN '20' THEN
              'AA'
             WHEN '21' THEN
              'BB'
             WHEN '22' THEN
              'BR'
       WHEN '23' THEN
        'OLG'
       WHEN '24' THEN
        'BPG'
             ELSE
              '?'
          END AS aardzakelijkrecht_akr_code
      ,blm.is_belast_met AS is_belast_met
      ,bel.belast AS belast
      -- onstaan_uit_ref and betrokken_bij_ref. populate later
      ,asg2.vve AS ontstaan_uit_ref -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs
      ,asg1.vve AS betrokken_bij_ref -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs
     ,asg2.id AS ontstaan_uit_asg_id
     ,asg1.id AS betrokken_bij_asg_id
    ,zrt.isbeperkt_tot
    ,asg1.id AS nrn_asg_id -- t.b.v. mview met actuele Zakelijk-rechtgegevens
      ,CASE
          WHEN asg2.vve IS NOT NULL
               AND asg1.vve IS NOT NULL THEN
           asg2.app_rechtsplitstype_code || ',' ||
           asg1.app_rechtsplitstype_code
          ELSE
           COALESCE(asg2.app_rechtsplitstype_code, asg1.app_rechtsplitstype_code)
       END AS asg_app_rechtsplitstype_code -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs
      ,CASE
          WHEN asg2.vve IS NOT NULL
               AND asg1.vve IS NOT NULL THEN
           ase2.omschrijving || ',' || ase1.omschrijving
          ELSE
           COALESCE(ase2.omschrijving, ase1.omschrijving)
       END AS asg_app_rechtsplitstype_oms -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs
       ,CASE WHEN asg2.id IS NOT NULL THEN 'nog niet beschikbaar' END-- BRK_EXTRACTIE_PCK.geef_einddatum_asg_fnc(tng.id)
                    AS asg_einddatum -- t.b.v. mview met actuele Zakelijk-rechtgegevens
      ,CASE WHEN asg2.id IS NOT NULL THEN 'nog niet beschikbaar' END -- BRK_EXTRACTIE_PCK.geef_asg_actueel_fnc(tng.id)
                      AS asg_actueel -- t.b.v. mview met actuele Zakelijk-rechtgegevens
      ,zrt.rust_op_kadastraalobject_id
      ,zrt.rust_op_kadastraalobj_volgnr
      ,kot.brk_kot_id AS Kadastraal_object_id
      ,kot.einddatum AS zrt_einddatum
      ,kot.creation AS zrt_begindatum
      ,kot.status_code AS kot_status_code
      ,kot.toestandsdatum       AS toestandsdatum
      ,kot.expiration_date AS expiration_date
FROM   brk.zakelijkrecht zrt
LEFT JOIN (
    SELECT
        zrt_id,
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) as is_belast_met
    FROM (
        SELECT
            zit.zakelijkrecht_id AS zrt_id,
            zrt2.identificatie AS identificatie
        FROM brk.zakelijkrecht_isbelastmet zit
        LEFT JOIN brk.zakelijkrecht zrt2
        ON zrt2.id = zit.is_belast_met
        ) sq
     GROUP BY zrt_id
) blm ON blm.zrt_id=zrt.id
LEFT JOIN (
    SELECT
        zrt_id,
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) as belast
    FROM (
        SELECT
            zit.is_belast_met AS zrt_id,
            zrt2.identificatie AS identificatie
        FROM brk.zakelijkrecht_isbelastmet zit
        LEFT JOIN brk.zakelijkrecht zrt2
        ON zrt2.id = zit.zakelijkrecht_id
    ) sq
    GROUP BY zrt_id
) bel ON bel.zrt_id=zrt.id
LEFT JOIN BRK.C_AARDZAKELIJKRECHT a             ON zrt.aardzakelijkrecht_code=a.code
LEFT JOIN brk_prep.kadastraal_object kot             ON (zrt.rust_op_kadastraalobject_id=kot.nrn_kot_id
                                                    AND zrt.rust_op_kadastraalobj_volgnr=kot.nrn_kot_volgnr)
LEFT JOIN brk.appartementsrechtsplitsing asg1   ON (asg1.id = zrt.betrokken_bij)
LEFT JOIN brk.appartementsrechtsplitsing asg2   ON (asg2.id = zrt.ontstaan_uit)
           -- ndg, 10-12-2015: workaround t.b.v. ophalen asg-informatie vves
LEFT JOIN brk.c_appartementsrechtsplitstype ase1 ON (ase1.code = asg1.app_rechtsplitstype_code)
LEFT JOIN brk.c_appartementsrechtsplitstype ase2 ON   (ase2.code = asg2.app_rechtsplitstype_code)
