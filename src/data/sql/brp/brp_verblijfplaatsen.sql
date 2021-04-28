select prs.systeem_nummer_persoon                         as identificatie,
       prs.straatnaam                                     as straatnaam,
       null                                               as openbare_ruimte,
       prs.huisnummer                                     as huisnummer,
       prs.huisletter                                     as huisletter,
       prs.huisnummer_toevoeging                          as huisnummertoevoeging,
       prs.huisnummer_aanduiding                          as aanduidinghuisnummer,
       prs.postcode_numeriek || prs.postcode_alfanumeriek as postcode,
       prs.adres_compleet                                 as adres_compleet,
       prs.naam_woongemeente                              as ligt_in_woonplaats,
       prs.omschrijving_locatie                           as locatiebeschrijving,
       prs.ident_verblijfplaats                           as heeft_verblijfsobject,
       prs.ident_nummeraanduiding                         as heeft_nummeraanduiding,
       null                                               as verblijft_in_land,
       null                                               as buitenland_regel_1,
       null                                               as buitenland_regel_2,
       null                                               as buitenland_regel_3,
       null                                               as datum_aanvang_adres_buitenland,
       prs.aanduiding_onderzoek                           as aanduiding_gegevens_in_onderzoek,
       prs.datum_begin_onderzoek                          as datum_ingang_onderzoek,
       prs.datum_einde_onderzoek                          as datum_einde_onderzoek,
       prs.aantal_keren_in_onderzoek                      as aantal_keren_in_onderzoek

from brp.personen_actueel as prs
where prs.ident_verblijfplaats is not null
  and prs.ident_nummeraanduiding is not null

union all

select ash.systeem_nummer_persoon        as identificatie,
       ash.straatnaam                    as straatnaam,
       null                              as openbare_ruimte,
       ash.huisnummer                    as huisnummer,
       ash.huisletter                    as huisletter,
       ash.huisnummer_toevoeging         as huisnummertoevoeging,
       null                              as aanduidinghuisnummer,
       ash.postcode                      as postcode,
       ash.adres_compleet                as adres_compleet,
       null                              as ligt_in_woonplaats,
       null                              as locatiebeschrijving,
       '0' || ash.ident_verblijfplaats   as heeft_verblijfsobject,
       '0' || ash.ident_nummeraanduiding as heeft_nummeraanduiding,
       null                              as verblijft_in_land,
       null                              as buitenland_regel_1,
       null                              as buitenland_regel_2,
       null                              as buitenland_regel_3,
       null                              as datum_aanvang_adres_buitenland,
       ash.aanduiding_onderzoek          as aanduiding_gegevens_in_onderzoek,
       ash.datum_begin_onderzoek_nr      as datum_ingang_onderzoek,
       ash.datum_einde_onderzoek_nr      as datum_einde_onderzoek,
       null                              as aantal_keren_in_onderzoek

from brp.adres_historisch as ash
where (
            ash.ident_verblijfplaats NOT IN (
            select prs.ident_verblijfplaats
            from brp.personen_actueel prs
        )
        OR
            ash.ident_nummeraanduiding NOT IN (
                select prs.ident_nummeraanduiding
                from brp.personen_actueel prs
            )
    )
  AND ash.ident_verblijfplaats IS NOT NULL
