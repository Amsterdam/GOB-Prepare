-- Bewoning_historisch kan meerdere rijen per persoon bevatten
-- Datum_overlijden is in dat geval gelijk
select bwh.systeem_nummer_persoon         as identificatie,
       substr(bwh.datum_overlijden, 0, 5) || '-' ||
       substr(bwh.datum_overlijden, 5, 2) || '-' ||
       substr(bwh.datum_overlijden, 7, 2) as datum_overlijden,
       null                               as plaats_overlijden,
       null                               as land_overlijden
from (
         select bwh.systeem_nummer_persoon, min(bwh.datum_overlijden) as datum_overlijden
         from brp.bewoning_historisch bwh
         where bwh.datum_overlijden is not null
         group by bwh.systeem_nummer_persoon
     ) bwh
