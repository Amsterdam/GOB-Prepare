UPDATE brk2_prep.kadastraal_object kot
SET is_ontstaan_uit_g_perceel = q.relatie_g_perceel
FROM (select kot.identificatie,
             kot.volgnummer,
             json_agg(
                     json_build_object(
                             'kot_id', kot2.id,
                             'kot_identificatie', kot2.identificatie,
                             'kot_volgnummer', kot2.volgnummer
                         ) ORDER BY kot2.identificatie, kot2.volgnummer
                 ) as relatie_g_perceel
      from brk2_prep.kadastraal_object kot
               join brk2.zakelijkrecht zrt
                    on kot.id = zrt.rust_op_kadastraalobject_id and
                       kot.volgnummer = zrt.rust_op_kadastraalobj_volgnr and
                       -- This looks like an unnecessary condition, but it makes a huge difference for the PG queryplanner
                       zrt.isontstaanuit_identificatie is not null
               join brk2.zakelijkrecht zrt2
                    on zrt2.isbetrokkenbij_identificatie = zrt.isontstaanuit_identificatie and
                        -- This looks like an unnecessary condition, but it makes a huge difference for the PG queryplanner
                       zrt.isbetrokkenbij_identificatie is not null
               join brk2_prep.kadastraal_object kot2
                    on zrt2.rust_op_kadastraalobject_id = kot2.id and
                       zrt2.rust_op_kadastraalobj_volgnr = kot2.volgnummer
      where kot.indexletter = 'A'
      group by kot.identificatie, kot.volgnummer) q
WHERE indexletter = 'A'
  AND kot.identificatie = q.identificatie
  AND kot.volgnummer = q.volgnummer
