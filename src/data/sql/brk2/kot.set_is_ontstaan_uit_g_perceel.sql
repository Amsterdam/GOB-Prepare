UPDATE brk2_prep.kadastraal_object
SET is_ontstaan_uit_g_perceel = q.relatie_g_perceel
FROM (select kot.identificatie,
             kot.volgnummer,
             json_agg(
                     json_build_object(
                             'kot_identificatie', q.kot2_identificatie
                         ) ORDER BY q.kot2_identificatie
                 ) as relatie_g_perceel
      from brk2_prep.kadastraal_object kot
               left join (select distinct zrt.rust_op_kadastraalobject_id  as kot_id,
                                          zrt.rust_op_kadastraalobj_volgnr as kot_volgnummer,
                                          kot2.identificatie               as kot2_identificatie
                          from brk2.zakelijkrecht zrt
                                   left join brk2.zakelijkrecht zrt2
                                             on zrt2.isbetrokkenbij_identificatie = zrt.isontstaanuit_identificatie
                                   left join brk2_prep.kadastraal_object kot2
                                             on zrt2.rust_op_kadastraalobject_id = kot2.id and
                                                zrt2.rust_op_kadastraalobj_volgnr = kot2.volgnummer) q
                         on kot.id = q.kot_id and kot.volgnummer = q.kot_volgnummer
      where kot.indexletter = 'A'
      group by kot.identificatie, kot.volgnummer
) q
WHERE indexletter = 'A';
