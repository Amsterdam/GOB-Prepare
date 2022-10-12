-- Use ST_DumpRings to filter the outer polygon ring and exclude holes
-- Collect the polygons and Use ST_UnaryUnion to remove conflicting linestrings
-- Take the exterior ring
-- The last union is to create MULTIPOLYGONs from areas with the same identificatie
select gc3.identificatie,
       ST_Union(gc3.geometrie) as geometrie,
       gc3.ligt_in_gemeente
from (
         select gc2.identificatie,
                ST_MakePolygon(ST_ExteriorRing((ST_Dump(gc2.geometrie)).geom)) as geometrie,
                gc2.ligt_in_gemeente
         from (
                  select gc1.identificatie,
                         gc1.ligt_in_gemeente,
                         ST_UnaryUnion(ST_Collect(gc1.geometrie)) as geometrie
                  from (
                           select aangeduid_door_kadastralegemeente_omschrijving as identificatie,
                                  (ST_DumpRings(geometrie)).path  as nrings,
                                  (ST_DumpRings(geometrie)).geom  as geometrie,
                                  gemeente as ligt_in_gemeente
                           from brk2_prepared.kadastraal_object
                           where indexletter = 'G'
                             and ST_IsValid(geometrie)
                             and datum_actueel_tot IS null
                       ) gc1
                  where gc1.nrings[1] = 0
                  group by gc1.identificatie,
                           gc1.ligt_in_gemeente
              ) gc2
     ) gc3
group by gc3.identificatie,
         gc3.ligt_in_gemeente
