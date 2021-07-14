select prs.systeem_nummer_persoon as identificatie,
--   prs.burgerservicenummer  as burgerservicenummer,
                            null  as burgerservicenummer,
         prs.datum_toetreding_gba as datum_eerste_inschrijving_GBA_RNI,
         prs.code_geheim          as indicatie_geheim
from brp.personen_actueel as prs
