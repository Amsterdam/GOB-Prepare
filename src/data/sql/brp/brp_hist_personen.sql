-- personen uit bewoning_historisch die niet actueel zijn
<<<<<<< refs/remotes/origin/AddHistorieBRP
select bwh.systeem_nummer_persoon as identificatie
,      bwh.burgerservicenummer    as burgerservicenummer
,      bwh.geboortedatum          as geboortedatum
,      bwh.administratienummer    as a_nummer
,      bwh.geslachtsaanduiding    as geslachtsaanduiding_code
from brp.bewoning_historisch bwh
where bwh.systeem_nummer_persoon not in 
     (select prs.systeem_nummer_persoon from personen_actueel prs)
=======
select bwh.systeem_nummer_persoon      as identificatie,
       bwh.burgerservicenummer         as burgerservicenummer,
       substr(bwh.geboortedatum, 0, 5) || '-' ||
       substr(bwh.geboortedatum, 5, 2) || '-' ||
       substr(bwh.geboortedatum, 7, 2) as geboortedatum,
       bwh.administratienummer         as a_nummer,
       bwh.geslachtsaanduiding         as geslachtsaanduiding_code
from brp.bewoning_historisch bwh
where bwh.systeem_nummer_persoon not in 
     (select prs.systeem_nummer_persoon from brp.personen_actueel prs)
>>>>>>> Add historie data for BRP import
