select wdt.wozbelobjnr || '.' || wdt.nr_deel                    as wozdeelobjectnummer,
       wdt.nr_deel                                              as deelnummer,
       wdt.wozbelobjnr                                          as wozbelobjnr,
       wdt.dsoc                                                 as soort_code,
       dsoc.omschrijving                                        as soort_omschrijving,
       wdt.ddingang_wozdeelobject                               as begin_geldigheid,
       case when wdt.tgo = 'VBO' then wdt.tgo_identificatie end as is_verbonden_met_verblijfsobject,
       case when wdt.tgo = 'LIG' then wdt.tgo_identificatie end as is_verbonden_met_ligplaats,
       case when wdt.tgo = 'STA' then wdt.tgo_identificatie end as is_verbonden_met_standplaats,
       case when wdt.tgo = 'PND' then wdt.tgo_identificatie end as heeft_pand
from woz.deel_object wdt
         left join woz.dsoc_codetabel dsoc on dsoc.dsoc = wdt.dsoc;