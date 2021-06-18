select prs.burgerservicenummer         as verbintenis_met_persoon1,
       prs.partner_burgerservicenummer as verbintenis_met_persoon2,
       prs.datum_huwelijk              as datum_sluiting,
       prs.datum_huwelijksontbinding   as datum_ontbinding,
       prs.burgerlijke_staat           as soort_verbintenis

from brp.personen_actueel as prs
-- filter: Geen huwelijk/geregistreerd partnerschap (ongehuwd)
where prs.burgerlijke_staat != 'O'
