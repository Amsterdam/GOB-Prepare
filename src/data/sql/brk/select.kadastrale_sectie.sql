-- To fix "non-noded" linestring intersections:
-- First take the UnaryUnion
-- Then Union the result per sectie
-- Lastly only take the outer boundary (ExteriorRing) to remove holes

select s.identificatie,
       s.code,
       ST_MakePolygon(ST_ExteriorRing((ST_Dump(s.geometrie)).geom)) as geometrie,
       s.is_onderdeel_van_kadastralegemeentecode
from (
         select kad_gemeentecode ->> 'omschrijving' || sectie as identificatie,
                sectie                                        as code,
                ST_Union(ST_UnaryUnion(geometrie))     as geometrie,
                kad_gemeentecode ->> 'omschrijving'           as is_onderdeel_van_kadastralegemeentecode
         from brk_prep.kadastraal_object
         where index_letter = 'G'
           and ST_IsValid(geometrie)
           and modification is null
           and status_code <> 'H'
         group by kad_gemeentecode ->> 'omschrijving',
                  sectie
     ) s
