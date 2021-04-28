-- actuele personen
SELECT *
-- pers.burgerservicenummer,
-- pers.a_nummer,
-- pers.voornamen,
-- pers.voorvoegsels,
-- pers.adellijketitel_predicaat,
-- pers.geslachtsnaam,
-- pers.geslachtsaanduiding_code,
-- pers.aanduiding_naamgebruik,
-- pers.geboortedatum,
-- pers.geboorteplaats_oms,
-- pers.geboorteland,
-- pers.nationaliteit,
-- pers.code_burgerlijke_staat,
-- pers.gezinsrelatie,
-- pers.aantal_kinderen,
-- pers.aantal_minderjarige_kinderen,
-- pers.heeft_verblijfplaatsen,
-- pers.heeft_verbintenis,
-- pers.heeft_inschrijving

FROM (
         select prs.burgerservicenummer         as burgerservicenummer,
                prs.administratienummer         as a_nummer,
                prs.voornamen                   as voornamen,
                prs.voorvoegsel                 as voorvoegsels,
                null                            as adellijketitel_predicaat,
                prs.geslachtsnaam               as geslachtsnaam,
                prs.geslachtsaanduiding         as geslachtsaanduiding_code,
                null                            as aanduiding_naamgebruik,
                prs.geboortedatum               as geboortedatum,
                prs.geboorteplaats              as geboorteplaats_oms,
                prs.geboorteland                as geboorteland,
                prs.nationaliteit               as nationaliteit,
                prs.burgelijke_staat            as code_burgerlijke_staat,
                json_build_object(
                        'code', prs.code_gezinsrelatie,
                        'omschrijving', prs.gezinsrelatie
                    )                           as gezinsrelatie,
                prs.aantal_kinderen             as aantal_kinderen,
                prs.waarvan_minderjarig         as aantal_minderjarige_kinderen,
                '0' || prs.ident_verblijfplaats as heeft_verblijfplaatsen,
                null                            as heeft_overlijden_gegevens,
                prs.partner_burgerservicenummer as heeft_verbintenis,
                prs.datum_toetreding_gba        as heeft_inschrijving

         from brp.personen_actueel prs
         where prs.ident_verblijfplaats is not null
           and prs.ident_nummeraanduiding is not null

         UNION ALL
-- historische personen
         select bwh.burgerservicenummer  as burgerservicenummer,
                bwh.administratienummer  as a_nummer,
                null                     as voornamen,
                null                     as voorvoegsels,
                null                     as adellijketitel_predicaat,
                null                     as geslachtsnaam,
                bwh.geslachtsaanduiding  as geslachtsaanduiding_code,
                null                     as aanduiding_naamgebruik,
                bwh.geboortedatum        as geboortedatum,
                null                     as geboorteplaats_oms,
                null                     as geboorteland,
                null                     as nationaliteit,
                null                     as code_burgerlijke_staat,
                null                     as gezinsrelatie,
                null                     as aantal_kinderen,
                null                     as aantal_minderjarige_kinderen,
                bwh.ident_verblijfplaats as heeft_verblijfplaatsen,
                bwh.datum_overlijden     as heeft_overlijden_gegevens,
                null                     as heeft_verbintenis,
                null                     as heeft_inschrijving

         from brp.bewoning_historisch bwh
         where bwh.indicatie_actueel_historie = 'H'
           AND bwh.burgerservicenummer NOT IN (
             select prs.burgerservicenummer
             from brp.personen_actueel prs
         )
     ) pers
-- heeft_overlijden_gegevens
-- left join brp.bewoning_historisch bwh
-- on pers.burgerservicenummer = bwh.burgerservicenummer
