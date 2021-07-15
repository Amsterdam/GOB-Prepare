select prs.systeem_nummer_persoon      as identificatie,
       prs2.systeem_nummer_persoon     as identificatie_persoon2,
--       prs.partner_administratienummer as anummer_persoon2,
       null                            as anummer_persoon2,
--       prs.partner_burgerservicenummer as bsn_persoon2,
       null                            as bsn_persoon2,

      split_part(prs.datum_huwelijk, '-', 3) || '-' ||
      split_part(prs.datum_huwelijk, '-', 2) || '-' ||
      split_part(prs.datum_huwelijk, '-', 1) as datum_sluiting,

      split_part(prs.datum_huwelijksontbinding, '-', 3) || '-' ||
      split_part(prs.datum_huwelijksontbinding, '-', 2) || '-' ||
      split_part(prs.datum_huwelijksontbinding, '-', 1) as datum_ontbinding

from brp.personen_actueel as prs
  left join brp.personen_actueel as prs2
  on  prs.partner_administratienummer = prs2.administratienummer
-- filter: Geen huwelijk/geregistreerd partnerschap (ongehuwd)
where prs.burgerlijke_staat != 'O'
and prs.partner_administratienummer is not null
