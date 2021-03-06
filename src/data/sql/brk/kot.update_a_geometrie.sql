-- Analyze database first.
ANALYZE;

--  This SQL statement updates the geometrie for kadastraal_object of type 'A'
--  end sets it to the geometrie of the first related verblijfsobject if
--  the geometrie of the verblijfsobject is in the union of related g-percelen
--
--  The clause 
--  AND kot1.sectie = kot2.sectie
--  AND kot1.kad_gemeentecode = kot2.kad_gemeentecode
--  was added to filter out G-percelen that are not in the same sectie.

WITH kot_g_poly AS (
SELECT nrn_kot_id, nrn_kot_volgnr, g_poly
    FROM (
        SELECT
            kot1.nrn_kot_id AS nrn_kot_id,
            kot1.nrn_kot_volgnr AS nrn_kot_volgnr,
            ST_Union(kot2.geometrie) AS g_poly
        FROM brk_prep.kadastraal_object kot1
        JOIN jsonb_array_elements(kot1.relatie_g_perceel) AS g_perceel
            ON kot1.relatie_g_perceel <> 'null'
            AND g_perceel->>'nrn_kot_id' IS NOT NULL
        JOIN brk_prep.kadastraal_object kot2
            ON kot2.nrn_kot_id =  (g_perceel->>'nrn_kot_id')::integer
            AND kot2.nrn_kot_volgnr =  (g_perceel->>'kot_volgnummer')::integer
            AND kot2.expiration_date IS NULL
        WHERE kot1.index_letter = 'A'
            AND kot1.expiration_date IS NULL
            AND kot1.sectie = kot2.sectie
            AND kot1.kad_gemeentecode = kot2.kad_gemeentecode
        GROUP BY kot1.nrn_kot_id, kot1.nrn_kot_volgnr) q1),
     vbo_kot_geometrie AS (
        SELECT distinct ON(kot.nrn_kot_id)
           kot.nrn_kot_id AS nrn_kot_id,
           kot.nrn_kot_volgnr AS nrn_kot_volgnr,
           vbo.geometrie AS geometrie
        FROM brk_prep.kadastraal_object kot
        JOIN jsonb_array_elements(kot.adressen) AS adres
            ON kot.adressen <> 'null'
            AND adres->>'bag_id' IS NOT NULL
        JOIN bag.verblijfsobjecten_geometrie vbo
            ON adres->>'bag_id' = vbo.identificatie
        LEFT JOIN kot_g_poly
            ON kot_g_poly.nrn_kot_id = kot.nrn_kot_id
            AND kot_g_poly.nrn_kot_volgnr = kot.nrn_kot_volgnr
        WHERE kot.index_letter = 'A'
            AND kot.expiration_date IS NULL
            AND vbo.geometrie IS NOT NULL
            AND (g_poly IS NULL OR ST_Within(vbo.geometrie, g_poly) = true))
UPDATE brk_prep.kadastraal_object
SET geometrie = vbo_kot_geometrie.geometrie
FROM vbo_kot_geometrie
WHERE brk_prep.kadastraal_object.nrn_kot_id = vbo_kot_geometrie.nrn_kot_id
    AND brk_prep.kadastraal_object.nrn_kot_volgnr = vbo_kot_geometrie.nrn_kot_volgnr
    AND brk_prep.kadastraal_object.geometrie IS NULL
;

--  Then we update kadastrale objects of type 'A' without geometrie
--  using ST_PointOnSurface of related G-percelen.


WITH point_g_poly AS (
    SELECT
        kot1.nrn_kot_id AS nrn_kot_id,
        kot1.nrn_kot_volgnr AS nrn_kot_volgnr,
        ST_PointOnSurface(ST_Union(kot2.geometrie)) AS geometrie
    FROM brk_prep.kadastraal_object kot1
    JOIN jsonb_array_elements(kot1.relatie_g_perceel) AS g_perceel
        ON kot1.relatie_g_perceel <> 'null'
        AND g_perceel->>'nrn_kot_id' IS NOT NULL
    JOIN LATERAL (
        SELECT DISTINCT ON (nrn_kot_id)
               nrn_kot_id, nrn_kot_volgnr, sectie, kad_gemeentecode, geometrie
            FROM brk_prep.kadastraal_object kot2
        ORDER BY nrn_kot_id, nrn_kot_volgnr DESC
        ) AS kot2
        ON kot2.nrn_kot_id = (g_perceel->>'nrn_kot_id')::integer
        AND kot2.nrn_kot_volgnr = (g_perceel->>'kot_volgnummer')::integer
    WHERE kot1.index_letter = 'A'
        AND kot1.expiration_date IS NULL
        AND kot1.sectie = kot2.sectie
        AND kot1.kad_gemeentecode = kot2.kad_gemeentecode
    GROUP BY kot1.nrn_kot_id, kot1.nrn_kot_volgnr)
UPDATE brk_prep.kadastraal_object
SET geometrie = point_g_poly.geometrie
FROM point_g_poly
WHERE brk_prep.kadastraal_object.nrn_kot_id = point_g_poly.nrn_kot_id
    AND brk_prep.kadastraal_object.nrn_kot_volgnr = point_g_poly.nrn_kot_volgnr
    AND brk_prep.kadastraal_object.geometrie IS NULL
;

--  Then we update kadastrale objects of type 'A' without geometrie
--  using geometrie of a "near" A-perceel (within the same complex).

WITH near_a_poly AS (
    SELECT DISTINCT ON(kot1.nrn_kot_id)
        kot1.nrn_kot_id,
        kot1.nrn_kot_volgnr AS nrn_kot_volgnr,
        kot2.geometrie
    FROM brk_prep.kadastraal_object kot1
    JOIN brk_prep.kadastraal_object kot2
        ON LEFT(kot1.kadastrale_aanduiding, -4) = LEFT(kot2.kadastrale_aanduiding, -4)
        AND kot2.expiration_date IS NULL
        AND kot2.geometrie IS NOT NULL
    WHERE kot1.geometrie IS NULL
        AND kot1.index_letter = 'A'
        AND kot1.expiration_date IS NULL
    ORDER BY
        kot1.nrn_kot_id,
        kot1.nrn_kot_volgnr,
        kot2.kadastrale_aanduiding)
UPDATE brk_prep.kadastraal_object
SET geometrie = near_a_poly.geometrie
FROM near_a_poly
WHERE brk_prep.kadastraal_object.nrn_kot_id = near_a_poly.nrn_kot_id
    AND brk_prep.kadastraal_object.nrn_kot_volgnr = near_a_poly.nrn_kot_volgnr
    AND brk_prep.kadastraal_object.geometrie IS NULL
;
