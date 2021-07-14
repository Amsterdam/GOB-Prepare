select bwh.systeem_nummer_persoon as identificatie,
       bwh.datum_overlijden       as datum_overlijden,
       null                       as plaats_overlijden,
       null                       as land_overlijden
from brp.bewoning_historisch bwh
where datum_overlijden is not null
