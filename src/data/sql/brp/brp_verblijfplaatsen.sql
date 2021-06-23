-- deduplicate on identificatie by using the row with highest aantal_keren_in_onderzoek
select distinct on (vbp.identificatie) *
from (
         select prs.systeemid_adres                                as identificatie,
                prs.straatnaam                                     as straatnaam,
                null                                               as openbare_ruimte,
                prs.huisnummer                                     as huisnummer,
                prs.huisletter                                     as huisletter,
                prs.huisnummer_toevoeging                          as huisnummertoevoeging,
                prs.huisnummer_aanduiding                          as aanduidinghuisnummer,
                prs.postcode_numeriek || prs.postcode_alfanumeriek as postcode,
                prs.adres_compleet                                 as adres_compleet,
                prs.woonplaats                                     as ligt_in_woonplaats,
                prs.omschrijving_locatie                           as locatiebeschrijving,

                case substr(prs.ident_verblijfplaats, 5, 2)
                    when '01' then prs.ident_verblijfplaats
                    end                                            as is_verblijfsobject,

                case substr(prs.ident_verblijfplaats, 5, 2)
                    when '02' then prs.ident_verblijfplaats
                    end                                            as is_ligplaats,

                case substr(prs.ident_verblijfplaats, 5, 2)
                    when '03' then prs.ident_verblijfplaats
                    end                                            as is_standplaats,

                prs.ident_nummeraanduiding                         as heeft_nummeraanduiding,
                null                                               as verblijft_in_land,
                null                                               as buitenland_regel_1,
                null                                               as buitenland_regel_2,
                null                                               as buitenland_regel_3,
                null                                               as datum_aanvang_adres_buitenland,
                prs.aanduiding_onderzoek                           as aanduiding_persoongegevens_in_onderzoek,
                prs.datum_begin_onderzoek                          as datum_ingang_persoononderzoek,
                prs.datum_einde_onderzoek                          as datum_einde_persoononderzoek,
                prs.aantal_keren_in_onderzoek                      as aantal_keren_persoon_in_onderzoek,
                prs.systeem_nummer_persoon || '.' || prs.systeemid_adres || '.' || prs.datum_adreshouding as heeft_persoonsverblijfplaatsen

         from brp.personen_actueel as prs
     ) vbp

order by vbp.identificatie, vbp.aantal_keren_persoon_in_onderzoek desc nulls last;
