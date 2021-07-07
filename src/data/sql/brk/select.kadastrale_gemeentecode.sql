-- The last union is to create MULTIPOLYGONs from multiple areas with the same identificatie
select gc2.identificatie,
       ST_Union(gc2.geometrie) as geometrie,
       gc2.is_onderdeel_van_kadastralegemeente
from (
         select gc1.identificatie,
                ST_MakePolygon(ST_ExteriorRing((ST_Dump(gc1.geometrie)).geom)) as geometrie,
                gc1.is_onderdeel_van_kadastralegemeente
         from (
                  select kad_gemeentecode ->> 'omschrijving' as identificatie,
                         ST_Union(ST_UnaryUnion(geometrie))  as geometrie,
                         kad_gemeente ->> 'omschrijving'     as is_onderdeel_van_kadastralegemeente
                  from brk_prep.kadastraal_object
                  where index_letter = 'G'
                    and ST_IsValid(geometrie)
                    and modification is NULL
                    and status_code <> 'H'
                  group by kad_gemeentecode ->> 'omschrijving', kad_gemeente ->> 'omschrijving'
              ) gc1
     ) gc2
group by gc2.identificatie, gc2.is_onderdeel_van_kadastralegemeente
