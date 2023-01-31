CREATE INDEX ON brk2_prep.zakelijk_recht (betrokken_bij_appartementsrechtsplitsing_vve);

CREATE TEMPORARY TABLE kot_g_perceel AS
SELECT kot.identificatie,
       kot.volgnummer,
       JSON_AGG(
               JSON_BUILD_OBJECT(
                       'kot_id', kot2.id,
                       'kot_identificatie', kot2.identificatie,
                       'kot_volgnummer', kot2.volgnummer
                   ) ORDER BY kot2.identificatie, kot2.volgnummer
           ) AS relatie_g_perceel
FROM brk2_prep.kadastraal_object kot
         JOIN brk2_prep.zakelijk_recht zrt
              ON zrt.betrokken_bij_appartementsrechtsplitsing_vve = kot.hoofdsplitsing_identificatie
         JOIN brk2_prep.kadastraal_object kot2
              ON zrt.__rust_op_kot_id = kot2.id AND
                 zrt.__rust_op_kot_volgnummer = kot2.volgnummer
WHERE kot.indexletter = 'A'
GROUP BY kot.identificatie, kot.volgnummer;

UPDATE brk2_prep.kadastraal_object kot
SET is_ontstaan_uit_brk_g_perceel = t.relatie_g_perceel
FROM kot_g_perceel t
WHERE indexletter = 'A'
  AND kot.identificatie = t.identificatie
  AND kot.volgnummer = t.volgnummer;

