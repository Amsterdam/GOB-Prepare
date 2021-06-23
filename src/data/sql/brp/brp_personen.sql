-- actuele personen
select prs.systeem_nummer_persoon      as identificatie,
--        prs.burgerservicenummer         as burgerservicenummer,
       null                            as burgerservicenummer,
--        prs.administratienummer         as a_nummer,
       null                            as a_nummer,
--        prs.voornamen                   as voornamen,
       null                            as voornamen,
--        prs.voorvoegsel                 as voorvoegsels,
       null                            as voorvoegsels,
       null                            as adellijketitel_predicaat,
--        prs.geslachtsnaam               as geslachtsnaam,
       null                            as geslachtsnaam,
       prs.geslachtsaanduiding         as geslachtsaanduiding_code,
       null                            as geslachtsaanduiding_oms,
       null                            as aanduiding_naamgebruik,
--       split_part(prs.geboortedatum, '-', 3) || '-' || split_part(prs.geboortedatum, '-', 2) ||
--        '-' || split_part(prs.geboortedatum, '-', 1) as geboortedatum,
       null                            as geboortedatum,
--         prs.geboorteplaats              as geboorteplaats_oms,
       null                            as geboorteplaats_oms,
       null                            as geboorteplaats_pl_in_buitlnd,
       null                            as geboorteplaats_heeft_gem_code,
--         prs.geboorteland                as geboorteland,
       null                            as geboorteland,
       prs.nationaliteit               as nationaliteit_oms,
       null                            as nationaliteit_code,
       prs.burgerlijke_staat           as code_burgerlijke_staat,
       prs.code_gezinsrelatie          as gezinsrelatie_code,
       prs.gezinsrelatie               as gezinsrelatie_oms,
       prs.aantal_kinderen             as aantal_kinderen,
       prs.waarvan_minderjarig         as aantal_minderjarige_kinderen,
       case
           when prs.datum_adreshouding = '00-00-0000'
               then prs.systeem_nummer_persoon||'.'||prs.systeemid_adres||'.'||prs.datum_toetreding_gba
               else prs.systeem_nummer_persoon||'.'||prs.systeemid_adres||'.'||prs.datum_adreshouding
       end                             as heeft_persoonsverblijfplaatsen,
       null                            as heeft_overlijden_gegevens,
       prs.partner_burgerservicenummer as heeft_verbintenis,
       null                            as heeft_inschrijving
from brp.personen_actueel prs
