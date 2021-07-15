select prs.systeem_nummer_persoon                   as identificatie,
--   prs.burgerservicenummer  as burgerservicenummer,
       null                                         as burgerservicenummer,

       split_part(prs.datum_toetreding_gba, '-', 3) || '-' ||
       split_part(prs.datum_toetreding_gba, '-', 2) || '-' ||
       split_part(prs.datum_toetreding_gba, '-', 1) as datum_eerste_inschrijving_GBA_RNI,

       prs.code_geheim                              as indicatie_geheim
from brp.personen_actueel as prs
