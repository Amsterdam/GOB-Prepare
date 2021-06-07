-- alleen van actuele personen met ingevulde verblijfplaats
select prs.burgerservicenummer  as burgerservicenummer,
       prs.datum_toetreding_gba as datum_eerste_inschrijving_GBA_RNI,
       prs.code_geheim          as indicatie_geheim
from brp.personen_actueel as prs
where prs.ident_verblijfplaats is not null and prs.ident_nummeraanduiding is not null
