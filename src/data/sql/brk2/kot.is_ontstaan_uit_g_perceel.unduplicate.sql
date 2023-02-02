ALTER TABLE brk2_prep.kot_ontstaan_uit_g_perceel
    RENAME TO kot_ontstaan_uit_g_perceel_doubles;

CREATE TABLE brk2_prep.kot_ontstaan_uit_g_perceel AS
SELECT kot_id,
       kot_volgnummer,
       ARRAY_TO_JSON(
               ARRAY_AGG(
                       JSON_BUILD_OBJECT(
                               'kot_identificatie', kot_identificatie
                           )
                   ) ORDER BY kot_identificatie
           )::jsonb AS is_ontstaan_uit_brk_g_perceel
FROM (SELECT kot.kot_id,
             kot.kot_volgnummer,
             gperc ->> 'kot_identificatie' AS kot_identificatie
      FROM brk2_prep.kot_ontstaan_uit_g_perceel_doubles kot
               JOIN JSON_ARRAY_ELEMENTS(kot.is_ontstaan_uit_brk_g_perceel) gperc ON TRUE
      WHERE kot.is_ontstaan_uit_brk_g_perceel IS NOT NULL
      GROUP BY kot_id, kot_volgnummer, gperc ->> 'kot_identificatie') q
GROUP BY kot_id, kot_volgnummer;