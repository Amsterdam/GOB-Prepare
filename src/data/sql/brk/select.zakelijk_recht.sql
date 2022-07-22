SELECT zrt.id																														--blijft
      ,zrt.identificatie																											--blijft
      ,zrt.aardzakelijkrecht_code																									--blijft
      ,a.omschrijving AS aardzakelijkrecht_oms																						--blijft
      ,CASE aardzakelijkrecht_code --bron:http://www.kadaster.nl/schemas/waardelijsten/AardZakelijkRecht/AardZakelijkRecht.gc 		--blijft
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
       END AS aardzakelijkrecht_akr_code																							--blijft
      ,blm.is_belast_met AS is_belast_met
      ,bel.belast AS belast
      ,zrt.isontstaanuit_identificatie AS ontstaan_uit_ref      --jr, 13-7-2022: nu een directe link naar de app.splitsing
      ,zrt.isbetrokkenbij_identificatie AS betrokken_bij_ref      --jr, 13-7-2022: nu een directe link naar de app.splitsing
     ,asg2.id AS ontstaan_uit_asg_id							--jr, 20-7-2022: misschien wel overbodig nu
     ,asg1.id AS betrokken_bij_asg_id							--jr, 20-7-2022: misschien wel overbodig nu
    ,ztt.isbeperkt_tot											--jr, 18-7-2022: via andere join
    ,asg1.id AS nrn_asg_id -- t.b.v. mview met actuele Zakelijk-rechtgegevens
      ,CASE
          WHEN asg2.identificatie IS NOT NULL     --nu via identificatie ipv vve
               AND asg1.identificatie IS NOT NULL THEN      --nu via identificatie ipv vve
           asg2.app_rechtsplitstype_code || ',' ||
           asg1.app_rechtsplitstype_code
          ELSE
           COALESCE(asg2.app_rechtsplitstype_code, asg1.app_rechtsplitstype_code)
       END AS asg_app_rechtsplitstype_code -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs  --jr, 19-7-2022 nog steeds nodig denk ik
      ,CASE
          WHEN asg2.identificatie IS NOT NULL --nu via identificatie ipv vve
               AND asg1.identificatie IS NOT NULL THEN --nu via identificatie ipv vve
           ase2.omschrijving || ',' || ase1.omschrijving
          ELSE
           COALESCE(ase2.omschrijving, ase1.omschrijving)
       END AS asg_app_rechtsplitstype_oms -- NDG, 10-12-2015: workaround t.b.v. ophalen ASG-informatie VVEs --jr, 19-7-2022 nog steeds nodig denk ik
       ,CASE WHEN asg2.id IS NOT NULL THEN 'nog niet beschikbaar' END-- BRK_EXTRACTIE_PCK.geef_einddatum_asg_fnc(tng.id)
                    AS asg_einddatum -- t.b.v. mview met actuele Zakelijk-rechtgegevens  --jr, 19-7-2022 nog steeds nodig denk ik
      ,CASE WHEN asg2.id IS NOT NULL THEN 'nog niet beschikbaar' END -- BRK_EXTRACTIE_PCK.geef_asg_actueel_fnc(tng.id)
                      AS asg_actueel -- t.b.v. mview met actuele Zakelijk-rechtgegevens --jr, 19-7-2022 nog steeds nodig denk ik
      ,asg1.vve_identificatie AS vve_identificatie_ontstaan_uit   --jr, 20-7-2022 nieuw
	  ,asg2.vve_identificatie AS vve_identificatie_betrokken_bij   --jr, 20-7-2022 nieuw
	  ,zrt.isbestemdtot_identificatie														--jr, 19-7-2022	nieuw
	  ,zrt.toelichting_bewaarder														--jr, 19-7-2022	nieuw
	  ,zrt.rust_op_kadastraalobject_id																							--blijft
      ,zrt.rust_op_kadastraalobj_volgnr																							--blijft
      ,kot.brk_kot_id AS Kadastraal_object_id
      ,kot.einddatum AS zrt_einddatum
      ,kot.creation AS zrt_begindatum
      ,kot.status_code AS kot_status_code
      ,kot.toestandsdatum       AS toestandsdatum
      ,kot.expiration_date AS expiration_date
      ,kot.creation AS creation
      ,kot.modification AS modification
	  ,agn.omschrijving AS inonderzoek_omschrijving				--jr,19-7-2022 nieuw
FROM   brk.zakelijkrecht zrt
LEFT JOIN (
    SELECT
        zrt_id,
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) as is_belast_met
    FROM (
        SELECT
            zit.zakelijkrecht_id AS zrt_id,													--blijft
            zrt2.identificatie AS identificatie												--blijft
        FROM brk.zakelijkrecht_isbelastmet zit     											--blijft
        LEFT JOIN brk.zakelijkrecht zrt2													--blijft
        ON zrt2.id = zit.isbelastmet_id			
        ) sq
     GROUP BY zrt_id
) blm ON blm.zrt_id=zrt.id
LEFT JOIN (
    SELECT
        zrt_id,
        array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) as belast
    FROM (
        SELECT
            zit.isbelastmet_id AS zrt_id,
            zrt2.identificatie AS identificatie
        FROM brk.zakelijkrecht_isbelastmet zit
        LEFT JOIN brk.zakelijkrecht zrt2
        ON zrt2.id = zit.zakelijkrecht_id
    ) sq
    GROUP BY zrt_id
) bel ON bel.zrt_id=zrt.id
LEFT JOIN brk.zakelijkrecht_isbeperkttot ztt     ON zrt.id=ztt.zakelijkrecht_id					--nu als aparte tabel geleverd
LEFT JOIN brk.c_aardzakelijkrecht a             ON zrt.aardzakelijkrecht_code=a.code			--blijft
LEFT JOIN brk_prep.kadastraal_object kot             ON (zrt.rust_op_kadastraalobject_id=kot.nrn_kot_id
                                                    AND zrt.rust_op_kadastraalobj_volgnr=kot.nrn_kot_volgnr)  --blijft
LEFT JOIN brk.appartementsrechtsplitsing asg1   ON (asg1.identificatie = zrt.isbetrokkenbij_identificatie)  --jr, 13-07-2022 loopt nu via de landelijke-vve-id
LEFT JOIN brk.appartementsrechtsplitsing asg2   ON (asg2.identificatie = zrt.isontstaanuit_identificatie)   --jr, 13-07-2022 loopt nu via de landelijke-vve-id
           -- ndg, 10-12-2015: workaround t.b.v. ophalen asg-informatie vves
LEFT JOIN brk.c_appartementsrechtsplitstype ase1 ON (ase1.code = asg1.app_rechtsplitstype_code)   --jr,20-7-2022: tabel lijkt niet te bestaan, maar is wel nodig
LEFT JOIN brk.c_appartementsrechtsplitstype ase2 ON   (ase2.code = asg2.app_rechtsplitstype_code)   --jr,20-7-2022: tabel lijkt niet te bestaan, maar is wel nodig
LEFT JOIN brk.zakelijkrecht_onderzoek zok ON (zok.zakelijkrecht_id=zrt.id)
LEFT JOIN brk.inonderzoek iok ON (iok.identificatie=zok.onderzoek_identificatie)
LEFT JOIN brk.c_authentiekgegeven agn ON (agn.code=ion.authentiekgegeven_code)


--nog doen:
--zakelijkrecht_isgebaseerdop: komt die bij stukdelen terug? denk het wel
--zakelijkrecht_erfpachtcanon, moet nog toegevoegd worden, maar hoe? - later uitzoeken