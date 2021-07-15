select prs.systeem_nummer_persoon || '.' || 'ouder1' as identificatie,
--        prs.ouder1_bsn                          as burgerservicenummer,
       null                                          as burgerservicenummer,
--        prs.ouder1_voornaam                     as voornamen,
       null                                          as voornamen,
--        prs.ouder1_voorvoegsel                  as voorvoegsels,
       null                                          as voorvoegsels,
--        prs.ouder1_geslachtsnaam                as geslachtsnaam,
       null                                          as geslachtsnaam,
       prs.ouder1_geslachtsaanduiding                as geslachtsaanduiding_code,
       null                                          as geslachtsaanduiding_oms,
--        prs.ouder1_geboortedatum                as geboortedatum,
       null                                          as geboortedatum,
--         prs.ouder1_geboorteplaats              as geboorteplaats_oms,
       null                                          as geboorteplaats_oms,
       null                                          as geboorteplaats_pl_in_buitlnd,
       null                                          as geboorteplaats_heeft_gem_code,
--         prs.ouder1_geboorteland                as geboorteland,
       null                                          as geboorteland

from brp.personen_actueel prs
where ouder1_geslachtsnaam is not null
  and trim(ouder1_geslachtsnaam) not in ('', '.')

union all

select prs.systeem_nummer_persoon || '.' || 'ouder2' as identificatie,
       --        prs.ouder2_bsn                          as burgerservicenummer,
       null                                          as burgerservicenummer,
       --        prs.ouder2_voornamen                    as voornamen,
       null                                          as voornamen,
       --        prs.ouder2_voorvoegsel                  as voorvoegsels,
       null                                          as voorvoegsels,
--        prs.ouder2_geslachtsnaam                       as geslachtsnaam,
       null                                          as geslachtsnaam,
       prs.ouder2_geslachtsaanduiding                as geslachtsaanduiding_code,
       null                                          as geslachtsaanduiding_oms,
       --        prs.ouder2_geboortedatum                as geboortedatum,
       null                                          as geboortedatum,
       --         prs.ouder2_geboorteplaats              as geboorteplaats_oms,
       null                                          as geboorteplaats_oms,
       null                                          as geboorteplaats_pl_in_buitlnd,
       null                                          as geboorteplaats_heeft_gem_code,
       --         prs.ouder2_geboorteland                as geboorteland,
       null                                          as geboorteland
from brp.personen_actueel prs
where ouder2_geslachtsnaam is not null
  and trim(ouder2_geslachtsnaam) not in ('', '.')

