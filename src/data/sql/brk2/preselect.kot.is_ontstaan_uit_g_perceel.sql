CREATE TABLE brk2_prep.kot_ontstaan_uit_g_perceel AS
SELECT kot.id         AS kot_id,
       kot.volgnummer AS kot_volgnummer,
       JSON_AGG(
               JSON_BUILD_OBJECT(
                       'kot_id', kot2.id,
                       'kot_identificatie', kot2.identificatie,
                       'kot_volgnummer', kot2.volgnummer
                   ) ORDER BY kot2.identificatie, kot2.volgnummer
           )::jsonb   AS is_ontstaan_uit_brk_g_perceel
FROM brk2_prep.kadastraal_object kot
         JOIN brk2_prep.zakelijk_recht zrt
              ON zrt.betrokken_bij_appartementsrechtsplitsing_vve = kot.hoofdsplitsing_identificatie
         JOIN brk2_prep.kadastraal_object kot2
              ON zrt.__rust_op_kot_id = kot2.id AND
                 zrt.__rust_op_kot_volgnummer = kot2.volgnummer
WHERE kot.indexletter = 'A'
GROUP BY kot.id, kot.volgnummer;

ANALYZE brk2_prep.kot_ontstaan_uit_g_perceel;

CREATE INDEX ON brk2_prep.kot_ontstaan_uit_g_perceel ((is_ontstaan_uit_brk_g_perceel->>'kot_id'), (is_ontstaan_uit_brk_g_perceel->>'kot_volgnummer'));
CREATE INDEX ON brk2_prep.kot_ontstaan_uit_g_perceel ((is_ontstaan_uit_brk_g_perceel->>'kot_id'));
CREATE INDEX ON brk2_prep.kot_ontstaan_uit_g_perceel ((is_ontstaan_uit_brk_g_perceel->>'kot_volgnummer'));
CREATE INDEX ON brk2_prep.kot_ontstaan_uit_g_perceel (kot_id, kot_volgnummer);
