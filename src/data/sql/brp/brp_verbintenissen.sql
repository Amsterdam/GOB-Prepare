select prs.systeem_nummer_persoon      as identificatie,
       prs2.systeem_nummer_persoon     as identificatie_persoon2,
--       prs.partner_administratienummer as anummer_persoon2,
       null                            as anummer_persoon2,
--       prs.partner_burgerservicenummer as bsn_persoon2,
       null                            as bsn_persoon2,
       prs.datum_huwelijk              as datum_sluiting,
       prs.datum_huwelijksontbinding   as datum_ontbinding
from brp.personen_actueel as prs
  left join brp.personen_actueel as prs2
  on  prs.partner_administratienummer = prs2.administratienummer
-- filter: Geen huwelijk/geregistreerd partnerschap (ongehuwd)
where prs.burgerlijke_staat != 'O'
and prs.partner_administratienummer is not null 

