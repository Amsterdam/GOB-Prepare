-- Historische adressen kunnen voorkomen die niet in actueel_persoon staan
-- Verblijfplaatsen worden daarom nogmaals toegevoegd
-- Datum onderzoek is altijd de laatste waarde
select ha.systeemid_adres                          as identificatie,
    ha.straatnaam                                  as straatnaam,
    ha.huisnummer                                  as huisnummer,
    ha.huisletter                                  as huisletter,
    ha.huisnummer_toevoeging                       as huisnummertoevoeging,
    ha.postcode                                    as postcode,
    ha.adres_compleet                              as adres_compleet,
    'Amsterdam'                                    as ligt_in_woonplaats,
    case substr(ha.ident_verblijfplaats, 5, 2)
        when '01' then ha.ident_verblijfplaats
        end                                        as is_verblijfsobject,
    case substr(ha.ident_verblijfplaats, 5, 2)
        when '02' then ha.ident_verblijfplaats
        end                                        as is_ligplaats,
    case substr(ha.ident_verblijfplaats, 5, 2)
        when '03' then ha.ident_verblijfplaats
        end                                        as is_standplaats,
    ha.ident_nummeraanduiding                      as heeft_nummeraanduiding,
    ha.aanduiding_onderzoek                        as aanduiding_persoongegevens_in_onderzoek,
    ha.datum_begin_onderzoek_nr                    as datum_ingang_persoononderzoek,
    ha.datum_einde_onderzoek_nr                    as datum_einde_persoononderzoek,
from brp.adres_historisch as ha
where ha.systeemid_adres not in ( select prs.systeemid_adres from brp.personen_actueel prs)
union all 
-- adressen uit bewoners_historie die niet in actueel en historische adressen bestaan (meestal null)
select bwh.systeemid_adres                         as identificatie,
    bwh.straatnaam                                 as straatnaam,
    bwh.huisnummer                                 as huisnummer,
    bwh.huisletter                                 as huisletter,
    bwh.huisnummer_toevoeging                      as huisnummertoevoeging,
    bwh.postcode                                   as postcode,
    null                                           as adres_compleet,
    'Amsterdam'                                    as ligt_in_woonplaats,
    case substr(bwh.ident_verblijfplaats, 5, 2)
        when '01' then bwh.ident_verblijfplaats
        end                                        as is_verblijfsobject,
    case substr(bwh.ident_verblijfplaats, 5, 2)
        when '02' then bwh.ident_verblijfplaats
        end                                        as is_ligplaats,
    case substr(bwh.ident_verblijfplaats, 5, 2)
        when '03' then bwh.ident_verblijfplaats
        end                                        as is_standplaats,
    bwh.ident_nummeraanduiding                     as heeft_nummeraanduiding,
    bwh.aanduiding_gegevens_onderzoek              as aanduiding_persoongegevens_in_onderzoek,
    bwh.datum_ingang_onderzoek                     as datum_ingang_persoononderzoek,
    bwh.datum_einde_onderzoek                      as datum_einde_persoononderzoek,
from brp.bewoning_historisch bwh 
where bwh.systeemid_adres not in ( select prs.systeemid_adres from brp.personen_actueel prs)
and bwh.systeemid_adres not in ( select ah.systeemid_adres from brp.adres_historisch ah)

