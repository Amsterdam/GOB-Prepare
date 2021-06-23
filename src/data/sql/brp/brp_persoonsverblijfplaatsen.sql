select prs.van_persoon || '.' || prs.met_verblijfplaats || '.' || prs.datum_aanvang_adreshouding as identificatie,
       prs.van_persoon,
       prs.met_verblijfplaats,
       prs.functieadres,
       prs.gemeente_inschrijving,
       prs.datum_inschrijving_gemeente,
       prs.datum_aanvang_adreshouding,
       prs.eind_datum_bewoning,
       prs.reden_einde_bewoning,
       prs.datum_uitschrijving_gemeente,
       prs.gemeente_waarnaar_vertrokken,
       prs.indicatie_actueel_historisch,
       prs.land_vanwaar_ingeschreven,
       prs.datum_vertrek_nederland,
       prs.land_waarnaar_vertrokken,
       prs.datum_vestiging_nederland
from (
         select prs.systeem_nummer_persoon       as van_persoon,
                prs.systeemid_adres              as met_verblijfplaats,
                prs.adres_compleet               as adres_compleet,
                prs.functieadres                 as functieadres,
                prs.gemeente_inschrijving        as gemeente_inschrijving,
                case
                    when prs.gemeente_inschrijving_datum = '00-00-0000'
                        then prs.datum_toetreding_gba
                    else prs.gemeente_inschrijving_datum
                    end                          as datum_inschrijving_gemeente,
                case
                    when prs.datum_adreshouding = '00-00-0000'
                        then prs.datum_toetreding_gba
                    else prs.datum_adreshouding
                    end                          as datum_aanvang_adreshouding,
                null                             as eind_datum_bewoning,
                null                             as reden_einde_bewoning,
                null                             as datum_uitschrijving_gemeente,
                null                             as gemeente_waarnaar_vertrokken,
                -- altijd actueel `A` uit actuele personen
                'A'                              as indicatie_actueel_historisch,
                prs.land_vanwaar_ingeschreven    as land_vanwaar_ingeschreven,
                null                             as datum_vertrek_nederland,
                null                             as land_waarnaar_vertrokken,
                prs.datum_vestiging_in_nederland as datum_vestiging_nederland
         from brp.personen_actueel as prs
     ) prs
