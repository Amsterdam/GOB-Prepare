select prs.bsn                   as burgerservicenummer,
       prs.administratienummer   as a_nummer,
       prs.voornamen             as voornamen,
       prs.voorvoegsel           as voorvoegsels,
       prs.geslachtsnaam         as geslachtsnaam,
       prs.geslachtsaanduiding   as geslachtsaanduiding_code,
       prs.geboortedatum         as geboortedatum,
       prs.geboorteplaats        as geboorteplaats_oms,
       prs.geboorteland          as geboorteland,
       prs.nationaliteit         as nationaliteit,
       prs.code_burgelijke_staat as code_burgerlijke_staat,
       json_build_object(
               'code', prs.code_gezinsrelatie,
               'omschrijving', prs.gezinsrelatie
           )                     as gezinsrelatie,
       prs.aantal_kinderen       as aantal_kinderen,
       prs.waarvan_minderjarig   as aantal_minderjarige_kinderen

from brp.personen_actueel prs