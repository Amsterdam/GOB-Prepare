-- Voeg historische verblijfplaatsen van een actueel persoon op een actueel amsterdams adres toe
-- aan de persoon - adres relatie tabel: persoonsverblijfplaatsen 
-- adres_historische bevat alleen historische gegevens dus kan volledig worden geimporteerd
select ha.systeem_nummer_persoon || '.' || ha.systeemid_adres || '.' || ha.datum_adreshouding_nr as identificatie,
       ha.systeem_nummer_persoon          as van_persoon,
       ha.systeemid_adres                 as met_verblijfplaats,
       ha.functieadres                    as functieadres,
       ha.gemeente_inschrijving           as gemeente_inschrijving,
       ha.datum_inschrijving_gemeentenr   as datum_inschrijving_gemeente,
       ha.datum_adreshouding_nr           as datum_aanvang_adreshouding,
       null                               as eind_datum_bewoning,
       null                               as reden_einde_bewoning,
       null                               as datum_uitschrijving_gemeente,
       null                               as gemeente_waarnaar_vertrokken,
       -- altijd actueel `H` uit historische adressen
       'H'                                as indicatie_actueel_historisch,
       ha.code_land_vanwaar_ingeschreven  as land_vanwaar_ingeschreven,
       ha.datum_vestiging_in_nederland_nr as datum_vestiging_nederland,
       null                               as land_waarnaar_vertrokken,
       null                               as datum_vertrek_nederland
from brp.adres_historisch as ha
union all
-- voeg historische persoonsverblijfplaatsen uit bewoning_historisch toe
-- voor verblijfsplaatsen waar een persoon niet in actueel_persoon zit
-- of de combi van persoon en adres niet in adres_historisch zit
select bwh.systeem_nummer_persoon || '.' || bwh.systeemid_adres || '.' || bwh.begindatum_bewoning as identificatie,
       bwh.systeem_nummer_persoon       as van_persoon,
       bwh.systeemid_adres              as met_verblijfplaats,
       bwh.functieadres                 as functieadres,
       null                             as gemeente_inschrijving,
       null                             as datum_inschrijving_gemeente,
       bwh.begindatum_bewoning          as datum_aanvang_adreshouding,
       bwh.einddatum_bewoning           as eind_datum_bewoning,
       bwh.reden_einde_bewoning         as reden_einde_bewoning,
       bwh.datum_uitschrijving_gemeente as datum_uitschrijving_gemeente,
       bwh.gemeente_waarnaar_vertrokken as gemeente_waarnaar_vertrokken,
       bwh.indicatie_actueel_historie   as indicatie_actueel_historie,
       null                             as land_vanwaar_ingeschreven,
       null                             as datum_vestiging_nederland,
       bwh.land_waarnaar_vertrokken     as land_waarnaar_vertrokken,
       bwh.datum_vertrek_nederland      as datum_vertrek_nederland,
from brp.bewoning_historisch as bwh
where (      bwh.systeem_nummer_persoon not in (select systeem_nummer_persoon from brp.personen_actueel)
        or   ( bwh.systeem_nummer_persoon||'.'|| bwh.systeemid_adres not in (select systeem_nummer_persoon ||'.'|| systeemid_adres from brp.adres_historisch ))
      )
