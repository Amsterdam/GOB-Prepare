-- personen uit bewoning_historisch die niet actueel zijn
select bwh.systeem_nummer_persoon as identificatie
,      bwh.burgerservicenummer    as burgerservicenummer
,      bwh.geboortedatum          as geboortedatum
,      bwh.administratienummer    as a_nummer
,      bwh.geslachtsaanduiding    as geslachtsaanduiding_code
from brp.bewoning_historisch bwh
where bwh.systeem_nummer_persoon not in 
     (select prs.systeem_nummer_persoon from personen_actueel prs)
