-- Use ST_DumpRings to filter the outer polygon ring and exclude holes
-- Collect the polygons and Use ST_UnaryUnion to remove conflicting linestrings
-- Take the exterior ring
-- The last union is to create MULTIPOLYGONs from areas with the same identificatie
SELECT gc3.identificatie       AS identificatie,
       ST_Union(gc3.geometrie) AS geometrie,
       gc3.ligt_in_gemeente    AS ligt_in_brk_gemeente,
       gc3.code                AS ligt_in_brk_gemeente_code
FROM (SELECT gc2.identificatie,
             ST_MakePolygon(ST_ExteriorRing((ST_Dump(gc2.geometrie)).geom)) AS geometrie,
             gc2.ligt_in_gemeente,
             gc2.code
      FROM (SELECT gc1.identificatie,
                   gc1.ligt_in_gemeente,
                   ST_UnaryUnion(ST_Collect(gc1.geometrie)) AS geometrie,
                   gc1.code
            FROM (SELECT aangeduid_door_brk_kadastralegemeente_omschrijving AS identificatie,
                         (ST_DumpRings(geometrie)).path                     AS nrings,
                         (ST_DumpRings(geometrie)).geom                     AS geometrie,
                         _gemeente                                          AS ligt_in_gemeente,
                         aangeduid_door_brk_kadastralegemeente_code         AS code
                  FROM brk2_prep.kadastraal_object
                  WHERE indexletter = 'G'
                    AND ST_IsValid(geometrie)
                    AND datum_actueel_tot IS NULL) gc1
            WHERE gc1.nrings[1] = 0
            GROUP BY gc1.identificatie,
                     gc1.ligt_in_gemeente,
                     gc1.code) gc2) gc3
GROUP BY gc3.identificatie,
         gc3.ligt_in_gemeente,
         gc3.code
