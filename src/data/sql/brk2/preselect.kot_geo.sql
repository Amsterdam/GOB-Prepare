--  This SQL statement updates the geometrie for kadastraal_object of type 'A'
--  end sets it to the geometrie of the first related verblijfsobject if
--  the geometrie of the verblijfsobject is in the union of related g-percelen
--
--  The clause
--  AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
--  AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
--  was added to filter out G-percelen that are not in the same aangeduid_door_kadastralesectie.
ANALYZE brk2_prep.kadastraal_object;
ANALYZE bag_brk2.verblijfsobjecten_geometrie;

-- Create table with union of geometries of related G-percelen for all ACTUAL A-percelen
CREATE TABLE brk2_prep.g_perceel_geo_union AS
SELECT kot1.id                  AS id,
       kot1.volgnummer          AS volgnummer,
       ST_Union(kot2.__geometrie) AS g_poly
FROM brk2_prep.kadastraal_object kot1
         JOIN JSONB_ARRAY_ELEMENTS(kot1.is_ontstaan_uit_g_perceel) AS g_perceel
              ON g_perceel ->> 'kot_id' IS NOT NULL
         JOIN brk2_prep.kadastraal_object kot2
              ON kot2.id = (g_perceel ->> 'kot_id')::integer
                  AND kot2.volgnummer = (g_perceel ->> 'kot_volgnummer')::integer
                  AND kot2._expiration_date IS NULL
WHERE kot1.indexletter = 'A'
  AND kot1._expiration_date IS NULL
  AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
  AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
GROUP BY kot1.id, kot1.volgnummer;

-- Create index on new table and analyze
CREATE INDEX ON brk2_prep.g_perceel_geo_union (id, volgnummer);
ANALYZE brk2_prep.g_perceel_geo_union;

-- Work from separate table
CREATE TABLE brk2_prep.kot_geo AS
SELECT id,
       volgnummer,
       __geometrie as geometrie,
       _expiration_date,
       indexletter,
       heeft_een_relatie_met_verblijfsobject,
       __kadastrale_aanduiding_minus_index_nummer
FROM brk2_prep.kadastraal_object;
CREATE INDEX ON brk2_prep.kot_geo (id, volgnummer);
CREATE INDEX ON brk2_prep.kot_geo (geometrie);
CREATE INDEX ON brk2_prep.kot_geo (_expiration_date);

-- 1. Set geometry for A-percelen based on related verblijfsobject. If A-perceel is in g_poly table, check if VOT
-- geometry falls within that polygon. Otherwise skip this check.
WITH vbo_kot_geometrie AS (SELECT DISTINCT ON (kot.id) kot.id         AS id,
                                                       kot.volgnummer AS volgnummer,
                                                       vbo.geometrie  AS geometrie
                           FROM brk2_prep.kot_geo kot
                                    JOIN JSONB_ARRAY_ELEMENTS(kot.heeft_een_relatie_met_verblijfsobject) AS adres
                                         ON adres ->> 'bag_id' IS NOT NULL
                                    JOIN bag_brk2.verblijfsobjecten_geometrie vbo
                                         ON adres ->> 'bag_id' = vbo.identificatie
                                    LEFT JOIN brk2_prep.g_perceel_geo_union kot_g_poly
                                              ON kot_g_poly.id = kot.id
                                                  AND kot_g_poly.volgnummer = kot.volgnummer
                           WHERE kot.indexletter = 'A'
                             AND kot._expiration_date IS NULL
                             AND vbo.geometrie IS NOT NULL
                             AND (g_poly IS NULL OR ST_Within(vbo.geometrie, g_poly) = TRUE))
UPDATE brk2_prep.kot_geo
SET geometrie = vbo_kot_geometrie.geometrie
FROM vbo_kot_geometrie
WHERE brk2_prep.kot_geo.id = vbo_kot_geometrie.id
  AND brk2_prep.kot_geo.volgnummer = vbo_kot_geometrie.volgnummer
  AND brk2_prep.kot_geo.geometrie IS NULL
;

-- 2. For all A-percelen that remain, set the geometry to point on surface of g_poly.
UPDATE brk2_prep.kot_geo
SET geometrie = ST_PointOnSurface(g_poly.g_poly)
FROM brk2_prep.g_perceel_geo_union g_poly
WHERE brk2_prep.kot_geo.id = g_poly.id
  AND brk2_prep.kot_geo.volgnummer = g_poly.volgnummer
  AND brk2_prep.kot_geo.geometrie IS NULL;


-- 3. Set geometry of remaining A-percelen based on nearby A-percelen (with same kadastrale aanduiding minus index nr)
WITH near_a_poly AS (SELECT DISTINCT ON (kot1.id) kot1.id,
                                                  kot1.volgnummer AS volgnummer,
                                                  kot2.geometrie
                     FROM brk2_prep.kot_geo kot1
                              JOIN brk2_prep.kot_geo kot2
                                   ON kot1.__kadastrale_aanduiding_minus_index_nummer =
                                      kot2.__kadastrale_aanduiding_minus_index_nummer
                                       AND kot2._expiration_date IS NULL
                                       AND kot2.geometrie IS NOT NULL
                     WHERE kot1.geometrie IS NULL
                       AND kot1.indexletter = 'A'
                       AND kot1._expiration_date IS NULL
                     ORDER BY kot1.id,
                              kot1.volgnummer,
                              kot1.__kadastrale_aanduiding_minus_index_nummer)
UPDATE brk2_prep.kot_geo
SET geometrie = near_a_poly.geometrie
FROM near_a_poly
WHERE brk2_prep.kot_geo.id = near_a_poly.id
  AND brk2_prep.kot_geo.volgnummer = near_a_poly.volgnummer
  AND brk2_prep.kot_geo.geometrie IS NULL
;
