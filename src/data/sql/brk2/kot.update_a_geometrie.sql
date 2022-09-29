--  This SQL statement updates the geometrie for kadastraal_object of type 'A'
--  end sets it to the geometrie of the first related verblijfsobject if
--  the geometrie of the verblijfsobject is in the union of related g-percelen
--
--  The clause
--  AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
--  AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
--  was added to filter out G-percelen that are not in the same aangeduid_door_kadastralesectie.

WITH kot_g_poly AS (
SELECT id, volgnummer, g_poly
    FROM (
        SELECT
            kot1.id AS id,
            kot1.volgnummer AS volgnummer,
            ST_Union(kot2.geometrie) AS g_poly
        FROM brk2_prep.kadastraal_object kot1
        JOIN jsonb_array_elements(kot1.is_ontstaan_uit_g_perceel) AS g_perceel
            ON kot1.is_ontstaan_uit_g_perceel IS NOT NULL
            AND g_perceel->>'kot_id' IS NOT NULL
        JOIN brk2_prep.kadastraal_object kot2
            ON kot2.id =  (g_perceel->>'kot_id')::integer
            AND kot2.volgnummer =  (g_perceel->>'kot_volgnummer')::integer
            AND kot2._expiration_date IS NULL
        WHERE kot1.indexletter = 'A'
            AND kot1._expiration_date IS NULL
            AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
            AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
        GROUP BY kot1.id, kot1.volgnummer) q1),
     vbo_kot_geometrie AS (
        SELECT distinct ON(kot.id)
           kot.id AS id,
           kot.volgnummer AS volgnummer,
           vbo.geometrie AS geometrie
        FROM brk2_prep.kadastraal_object kot
        JOIN jsonb_array_elements(kot.heeft_een_relatie_met_verblijfsobject) AS adres
            ON kot.heeft_een_relatie_met_verblijfsobject IS NOT NULL
            AND adres->>'bag_id' IS NOT NULL
        JOIN bag_brk2.verblijfsobjecten_geometrie vbo
            ON adres->>'bag_id' = vbo.identificatie
        LEFT JOIN kot_g_poly
            ON kot_g_poly.id = kot.id
            AND kot_g_poly.volgnummer = kot.volgnummer
        WHERE kot.indexletter = 'A'
            AND kot._expiration_date IS NULL
            AND vbo.geometrie IS NOT NULL
            AND (g_poly IS NULL OR ST_Within(vbo.geometrie, g_poly) = true))
UPDATE brk2_prep.kadastraal_object
SET geometrie = vbo_kot_geometrie.geometrie
FROM vbo_kot_geometrie
WHERE brk2_prep.kadastraal_object.id = vbo_kot_geometrie.id
    AND brk2_prep.kadastraal_object.volgnummer = vbo_kot_geometrie.volgnummer
    AND brk2_prep.kadastraal_object.geometrie IS NULL
;

--  Then we update kadastrale objects of type 'A' without geometrie
--  using ST_PointOnSurface of related G-percelen.


WITH point_g_poly AS (
    SELECT
        kot1.id AS id,
        kot1.volgnummer AS volgnummer,
        ST_PointOnSurface(ST_Union(kot2.geometrie)) AS geometrie
    FROM brk2_prep.kadastraal_object kot1
    JOIN jsonb_array_elements(kot1.is_ontstaan_uit_g_perceel) AS g_perceel
        ON kot1.is_ontstaan_uit_g_perceel IS NOT NULL
        AND g_perceel->>'kot_id' IS NOT NULL
    JOIN LATERAL (
        SELECT DISTINCT ON (id)
               id, volgnummer, aangeduid_door_kadastralesectie, aangeduid_door_kadastralegemeentecode_code, geometrie
            FROM brk2_prep.kadastraal_object kot2
        ORDER BY id, volgnummer DESC
        ) AS kot2
        ON kot2.id = (g_perceel->>'kot_id')::integer
        AND kot2.volgnummer = (g_perceel->>'kot_volgnummer')::integer
    WHERE kot1.indexletter = 'A'
        AND kot1._expiration_date IS NULL
        AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
        AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
    GROUP BY kot1.id, kot1.volgnummer)
UPDATE brk2_prep.kadastraal_object
SET geometrie = point_g_poly.geometrie
FROM point_g_poly
WHERE brk2_prep.kadastraal_object.id = point_g_poly.id
    AND brk2_prep.kadastraal_object.volgnummer = point_g_poly.volgnummer
    AND brk2_prep.kadastraal_object.geometrie IS NULL
;

--  Then we update kadastrale objects of type 'A' without geometrie
--  using geometrie of a "near" A-perceel (within the same complex).

WITH near_a_poly AS (
    SELECT DISTINCT ON(kot1.id)
        kot1.id,
        kot1.volgnummer AS volgnummer,
        kot2.geometrie
    FROM brk2_prep.kadastraal_object kot1
    JOIN brk2_prep.kadastraal_object kot2
        ON LEFT(kot1.kadastrale_aanduiding, -4) = LEFT(kot2.kadastrale_aanduiding, -4)
        AND kot2._expiration_date IS NULL
        AND kot2.geometrie IS NOT NULL
    WHERE kot1.geometrie IS NULL
        AND kot1.indexletter = 'A'
        AND kot1._expiration_date IS NULL
    ORDER BY
        kot1.id,
        kot1.volgnummer,
        kot2.kadastrale_aanduiding)
UPDATE brk2_prep.kadastraal_object
SET geometrie = near_a_poly.geometrie
FROM near_a_poly
WHERE brk2_prep.kadastraal_object.id = near_a_poly.id
    AND brk2_prep.kadastraal_object.volgnummer = near_a_poly.volgnummer
    AND brk2_prep.kadastraal_object.geometrie IS NULL
;


--  This SQL statement updates the geometrie for kadastraal_object of type 'A'
--  end sets it to the geometrie of the first related verblijfsobject if
--  the geometrie of the verblijfsobject is in the union of related g-percelen
--
--  The clause
--  AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
--  AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
--  was added to filter out G-percelen that are not in the same aangeduid_door_kadastralesectie.

WITH kot_g_poly AS (
SELECT id, volgnummer, g_poly
    FROM (
        SELECT
            kot1.id AS id,
            kot1.volgnummer AS volgnummer,
            ST_Union(kot2.geometrie) AS g_poly
        FROM brk2_prep.kadastraal_object kot1
        JOIN jsonb_array_elements(kot1.is_ontstaan_uit_g_perceel) AS g_perceel
            ON kot1.is_ontstaan_uit_g_perceel IS NOT NULL
            AND g_perceel->>'kot_id' IS NOT NULL
        JOIN brk2_prep.kadastraal_object kot2
            ON kot2.id =  (g_perceel->>'kot_id')::integer
            AND kot2.volgnummer =  (g_perceel->>'kot_volgnummer')::integer
            AND kot2._expiration_date IS NULL
        WHERE kot1.indexletter = 'A'
            AND kot1._expiration_date IS NULL
            AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
            AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
        GROUP BY kot1.id, kot1.volgnummer) q1),
     vbo_kot_geometrie AS (
        SELECT distinct ON(kot.id)
           kot.id AS id,
           kot.volgnummer AS volgnummer,
           vbo.geometrie AS geometrie
        FROM brk2_prep.kadastraal_object kot
        JOIN jsonb_array_elements(kot.heeft_een_relatie_met_verblijfsobject) AS adres
            ON kot.heeft_een_relatie_met_verblijfsobject IS NOT NULL
            AND adres->>'bag_id' IS NOT NULL
        JOIN bag_brk2.verblijfsobjecten_geometrie vbo
            ON adres->>'bag_id' = vbo.identificatie
        LEFT JOIN kot_g_poly
            ON kot_g_poly.id = kot.id
            AND kot_g_poly.volgnummer = kot.volgnummer
        WHERE kot.indexletter = 'A'
            AND kot._expiration_date IS NULL
            AND vbo.geometrie IS NOT NULL
            AND (g_poly IS NULL OR ST_Within(vbo.geometrie, g_poly) = true))
UPDATE brk2_prep.kadastraal_object
SET geometrie = vbo_kot_geometrie.geometrie
FROM vbo_kot_geometrie
WHERE brk2_prep.kadastraal_object.id = vbo_kot_geometrie.id
    AND brk2_prep.kadastraal_object.volgnummer = vbo_kot_geometrie.volgnummer
    AND brk2_prep.kadastraal_object.geometrie IS NULL
;

--  Then we update kadastrale objects of type 'A' without geometrie
--  using ST_PointOnSurface of related G-percelen.


WITH point_g_poly AS (
    SELECT
        kot1.id AS id,
        kot1.volgnummer AS volgnummer,
        ST_PointOnSurface(ST_Union(kot2.geometrie)) AS geometrie
    FROM brk2_prep.kadastraal_object kot1
    JOIN jsonb_array_elements(kot1.is_ontstaan_uit_g_perceel) AS g_perceel
        ON kot1.is_ontstaan_uit_g_perceel IS NOT NULL
        AND g_perceel->>'kot_id' IS NOT NULL
    JOIN LATERAL (
        SELECT DISTINCT ON (id)
               id, volgnummer, aangeduid_door_kadastralesectie, aangeduid_door_kadastralegemeentecode_code, geometrie
            FROM brk2_prep.kadastraal_object kot2
        ORDER BY id, volgnummer DESC
        ) AS kot2
        ON kot2.id = (g_perceel->>'kot_id')::integer
        AND kot2.volgnummer = (g_perceel->>'kot_volgnummer')::integer
    WHERE kot1.indexletter = 'A'
        AND kot1._expiration_date IS NULL
        AND kot1.aangeduid_door_kadastralesectie = kot2.aangeduid_door_kadastralesectie
        AND kot1.aangeduid_door_kadastralegemeentecode_code = kot2.aangeduid_door_kadastralegemeentecode_code
    GROUP BY kot1.id, kot1.volgnummer)
UPDATE brk2_prep.kadastraal_object
SET geometrie = point_g_poly.geometrie
FROM point_g_poly
WHERE brk2_prep.kadastraal_object.id = point_g_poly.id
    AND brk2_prep.kadastraal_object.volgnummer = point_g_poly.volgnummer
    AND brk2_prep.kadastraal_object.geometrie IS NULL
;

--  Then we update kadastrale objects of type 'A' without geometrie
--  using geometrie of a "near" A-perceel (within the same complex).

WITH near_a_poly AS (
    SELECT DISTINCT ON(kot1.id)
        kot1.id,
        kot1.volgnummer AS volgnummer,
        kot2.geometrie
    FROM brk2_prep.kadastraal_object kot1
    JOIN brk2_prep.kadastraal_object kot2
        ON LEFT(kot1.kadastrale_aanduiding, -4) = LEFT(kot2.kadastrale_aanduiding, -4)
        AND kot2._expiration_date IS NULL
        AND kot2.geometrie IS NOT NULL
    WHERE kot1.geometrie IS NULL
        AND kot1.indexletter = 'A'
        AND kot1._expiration_date IS NULL
    ORDER BY
        kot1.id,
        kot1.volgnummer,
        kot2.kadastrale_aanduiding)
UPDATE brk2_prep.kadastraal_object
SET geometrie = near_a_poly.geometrie
FROM near_a_poly
WHERE brk2_prep.kadastraal_object.id = near_a_poly.id
    AND brk2_prep.kadastraal_object.volgnummer = near_a_poly.volgnummer
    AND brk2_prep.kadastraal_object.geometrie IS NULL
;
