-- alleen van actuele personen met ingevulde verblijfplaats
select prs.burgerservicenummer         as verbintenis_met_persoon1,
       prs.partner_burgerservicenummer as verbintenis_met_persoon2,
       prs.datum_huwelijk              as datum_sluiting,
       prs.datum_huwelijksontbinding   as datum_ontbinding,
       prs.burgelijke_staat            as soort_verbintenis

from brp.personen_actueel as prs
-- filter: Geen huwelijk/geregistreerd partnerschap (ongehuwd)
where prs.burgelijke_staat != 'O'
  and prs.ident_verblijfplaats is not null
  and prs.ident_nummeraanduiding is not null
