select
	prs.systeem_nummer_persoon as identificatie,
	prs.burgerservicenummer as van_persoon,
	prs.ident_verblijfplaats as met_verblijfplaats,
	prs.functieadres as functieadres,
	prs.gemeente_inschrijving as gemeente_inschrijving,
	case
		when prs.gemeente_inschrijving_datum = '00-00-0000' then prs.datum_toetreding_gba
		else prs.gemeente_inschrijving_datum
	end as datum_inschrijving_gemeente,
	case
		when prs.datum_adreshouding = '00-00-0000' then prs.datum_toetreding_gba
		else prs.datum_adreshouding
	end as datum_aanvang_adreshouding,
	null as eind_datum_bewoning,
	null as reden_einde_bewoning,
	null as datum_uitschrijving_gemeente,
	null as gemeente_waarnaar_vertrokken,
	null as indicatie_actueel_historisch,
	prs.land_vanwaar_ingeschreven as land_vanwaar_ingeschreven,
	null as datum_vertrek_nederland,
	null as land_waarnaar_vertrokken,
	prs.datum_vestiging_in_nederland as datum_vestiging_nederland
from
	brp.personen_actueel as prs

union all

select
	null as identificatie,
	ash.burgerservicenummer as van_persoon,
	'0' || ash.ident_verblijfplaats as met_verblijfplaats,
	ash.functieadres as functieadres,
	ash.gemeente_inschrijving as gemeente_inschrijving,
	ash.datum_inschrijving_gemeentenr as datum_inschrijving_gemeente,
    ash.datum_adreshouding_nr as datum_aanvang_adreshouding,
	null as eind_datum_bewoning,
	null as reden_einde_bewoning,
	null as datum_uitschrijving_gemeente,
	null as gemeente_waarnaar_vertrokken,
	null as indicatie_actueel_historisch,
	ash.code_land_vanwaar_ingeschreven as land_vanwaar_ingeschreven,
	null as datum_vertrek_nederland,
	null as land_waarnaar_vertrokken,
	ash.datum_vestiging_in_nederland_nr as datum_vestiging_nederland

from brp.adres_historisch ash

union all

select
	null as identificatie,
	bwh.burgerservicenummer as van_persoon,
	bwh.ident_verblijfplaats as met_verblijfplaats,
	bwh.functieadres as functieadres,
	null as gemeente_inschrijving,
	null as datum_inschrijving_gemeente,
    bwh.begindatum_bewoning as datum_aanvang_adreshouding,
	bwh.einddatum_bewoning as eind_datum_bewoning,
	bwh.reden_einde_bewoning as reden_einde_bewoning,
	bwh.datum_uitschrijving_gemeente as datum_uitschrijving_gemeente,
	bwh.gemeente_waarnaar_vertrokken as gemeente_waarnaar_vertrokken,
	bwh.indicatie_actueel_historie as indicatie_actueel_historisch,
	null as land_vanwaar_ingeschreven,
	bwh.datum_vertrek_nederland as datum_vertrek_nederland,
	bwh.land_waarnaar_vertrokken as land_waarnaar_vertrokken,
	null as datum_vestiging_nederland

from brp.bewoning_historisch as bwh
