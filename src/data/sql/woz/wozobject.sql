select wot.wozbelobjnr               as wozobjectnummer,
       wot.gebruikscode              as gebruik_code,
       wot.gebruik_oms               as gebruik_oms,
       wot.hoofdcode_gebr            as soortobject_code,
       wot.hoofd_oms                 as soortobject_oms,
       wot.ddingang                  as begin_geldigheid,
       kad.kadastrale_identificaties as bevat_kadastraal_object
from woz.object wot
         left join (
    select kad.wozbelobjnr,
           array_to_json(array_agg(kad.kadastrale_identificatie)) as kadastrale_identificaties
    from woz.kad_relatie kad
    group by kad.wozbelobjnr
) kad on kad.wozbelobjnr = wot.wozbelobjnr;