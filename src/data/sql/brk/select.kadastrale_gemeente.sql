-- The last union is to create MULTIPOLYGONs from multiple areas with the same identificatie
select g2.identificatie,
       ST_Union(g2.geometrie) as geometrie,
       g2.ligt_in_gemeente
from (
         select g1.identificatie,
                ST_MakePolygon(ST_ExteriorRing((ST_Dump(g1.geometrie)).geom)) as geometrie,
                g1.ligt_in_gemeente
         from (
                  select kad_gemeente ->> 'omschrijving'    as identificatie,
                         ST_Union(ST_UnaryUnion(geometrie)) as geometrie,
                         brg_gemeente ->> 'omschrijving'    as ligt_in_gemeente
                  from brk_prep.kadastraal_object
                  where index_letter = 'G'
                    and ST_IsValid(geometrie)
                    and modification is NULL
                    and status_code <> 'H'
                  group by kad_gemeente ->> 'omschrijving', brg_gemeente ->> 'omschrijving'
              ) g1
     ) g2
group by g2.identificatie, g2.ligt_in_gemeente
