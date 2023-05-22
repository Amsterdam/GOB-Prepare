-- Use ST_DumpRings to filter the outer polygon ring and exclude holes
-- Collect the polygons and Use ST_UnaryUnion to remove conflicting linestrings
-- Take the exterior ring
-- The last union is to create MULTIPOLYGONs from areas with the same identificatie
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT gc3.identificatie,
       ST_Union(gc3.geometrie)                 AS geometrie,
       gc3.is_onderdeel_van_kadastralegemeente AS is_onderdeel_van_brk_kadastrale_gemeente,
       gc3.code                                AS code
FROM (SELECT gc2.identificatie,
             ST_MakePolygon(ST_ExteriorRing((ST_Dump(gc2.geometrie)).geom)) AS geometrie,
             gc2.is_onderdeel_van_kadastralegemeente,
             gc2.code
      FROM (SELECT gc1.identificatie,
                   gc1.is_onderdeel_van_kadastralegemeente,
                   ST_UnaryUnion(ST_Collect(gc1.geometrie)) AS geometrie,
                   gc1.code
            FROM (SELECT aangeduid_door_brk_kadastralegemeentecode_omschrijving AS identificatie,
                         (ST_DumpRings(geometrie)).path                         AS nrings,
                         (ST_DumpRings(geometrie)).geom                         AS geometrie,
                         aangeduid_door_brk_kadastralegemeente_omschrijving     AS is_onderdeel_van_kadastralegemeente,
                         aangeduid_door_brk_kadastralegemeentecode_code         AS code
                  FROM brk2_prep.kadastraal_object
                  WHERE indexletter = 'G'
                    AND ST_IsValid(geometrie)
                    AND datum_actueel_tot IS NULL) gc1
            WHERE gc1.nrings[1] = 0
            GROUP BY gc1.identificatie,
                     gc1.is_onderdeel_van_kadastralegemeente,
                     gc1.code) gc2) gc3
GROUP BY gc3.identificatie,
         gc3.is_onderdeel_van_kadastralegemeente,
         gc3.code
