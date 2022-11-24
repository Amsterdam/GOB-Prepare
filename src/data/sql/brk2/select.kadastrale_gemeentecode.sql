-- Use ST_DumpRings to filter the outer polygon ring and exclude holes
-- Collect the polygons and Use ST_UnaryUnion to remove conflicting linestrings
-- Take the exterior ring
-- The last union is to create MULTIPOLYGONs from areas with the same identificatie
SELECT gc3.identificatie,
       ST_Union(gc3.geometrie)                 AS geometrie,
       gc3.is_onderdeel_van_kadastralegemeente AS is_onderdeel_van_brk_kadastrale_gemeente
FROM (SELECT gc2.identificatie,
             ST_MakePolygon(ST_ExteriorRing((ST_Dump(gc2.geometrie)).geom)) AS geometrie,
             gc2.is_onderdeel_van_kadastralegemeente
      FROM (SELECT gc1.identificatie,
                   gc1.is_onderdeel_van_kadastralegemeente,
                   ST_UnaryUnion(ST_Collect(gc1.geometrie)) AS geometrie
            FROM (SELECT aangeduid_door_kadastralegemeentecode_omschrijving AS identificatie,
                         (ST_DumpRings(geometrie)).path                     AS nrings,
                         (ST_DumpRings(geometrie)).geom                     AS geometrie,
                         aangeduid_door_kadastralegemeente_omschrijving     AS is_onderdeel_van_kadastralegemeente
                  FROM brk2_prep.kadastraal_object
                  WHERE indexletter = 'G'
                    AND ST_IsValid(geometrie)
                    AND datum_actueel_tot IS NULL) gc1
            WHERE gc1.nrings[1] = 0
            GROUP BY gc1.identificatie,
                     gc1.is_onderdeel_van_kadastralegemeente) gc2) gc3
GROUP BY gc3.identificatie,
         gc3.is_onderdeel_van_kadastralegemeente
