-- Use ST_DumpRings to filter the outer polygon ring and exclude holes
-- Collect the polygons and Use ST_UnaryUnion to remove conflicting linestrings
-- Take the exterior ring
-- The last union is to create MULTIPOLYGONs from areas with the same identificatie
CREATE TABLE brk2_prep.kadastrale_gemeente AS
SELECT gc3.identificatie       AS identificatie,
       ST_Union(gc3.geometrie) AS geometrie,
       gc3.ligt_in_gemeente    AS ligt_in_brk_gemeente
FROM (SELECT gc2.identificatie,
             ST_MakePolygon(ST_ExteriorRing((ST_Dump(gc2.geometrie)).geom)) AS geometrie,
             gc2.ligt_in_gemeente
      FROM (SELECT gc1.identificatie,
                   gc1.ligt_in_gemeente,
                   ST_UnaryUnion(ST_Collect(gc1.geometrie)) AS geometrie
            FROM (SELECT aangeduid_door_kadastralegemeente_omschrijving AS identificatie,
                         (ST_DumpRings(geometrie)).path                 AS nrings,
                         (ST_DumpRings(geometrie)).geom                 AS geometrie,
                         gemeente                                       AS ligt_in_gemeente
                  FROM brk2_prep.kadastraal_object
                  WHERE indexletter = 'G'
                    AND ST_IsValid(geometrie)
                    AND datum_actueel_tot IS NULL) gc1
            WHERE gc1.nrings[1] = 0
            GROUP BY gc1.identificatie,
                     gc1.ligt_in_gemeente) gc2) gc3
GROUP BY gc3.identificatie,
         gc3.ligt_in_gemeente