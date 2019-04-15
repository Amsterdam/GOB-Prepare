create materialized view brk.baghulptabel as select
	kadastraalobject_id,
	kadastraalobject_volgnummer,
	string_agg(
	coalesce(bag_id, '') || '|' ||
	coalesce(openbareruimtenaam, '') || '|' ||
	coalesce(cast(huisnummer as varchar(255)), '') || '|' ||
	coalesce(huisletter, '') || '|' ||
	coalesce(huisnummertoevoeging, '') || '|' ||
	coalesce(postcode, '') || '|' ||
	coalesce(woonplaatsnaam, '')
	, ';') as adres
from (
SELECT kas.id
      ,kas.kadastraalobject_id
      ,kas.kadastraalobject_volgnummer
      ,aot.bag_id
      ,kas.openbareruimtenaam
      ,kas.huisnummer
      ,kas.huisletter
      ,kas.huisnummertoevoeging
      ,kas.postcode
      ,kas.woonplaatsnaam
FROM   brk.kadastraal_adres kas
LEFT JOIN   brk.adresseerbaar_object aot
ON     kas.adresseerbaar_object_id = aot.id
WHERE  kas.adresseerbaar_object_id IS null
UNION
SELECT kas.id
      ,kas.kadastraalobject_id
      ,kas.kadastraalobject_volgnummer
      ,aot.bag_id
      ,aot.openbareruimtenaam
      ,aot.huisnummer
      ,aot.huisletter
      ,aot.huisnummertoevoeging
      ,aot.postcode
      ,aot.woonplaatsnaam
FROM   brk.kadastraal_adres kas
LEFT JOIN   brk.adresseerbaar_object aot
ON     kas.adresseerbaar_object_id = aot.id
WHERE  kas.adresseerbaar_object_id IS NOT null
) adr
where not (bag_id is null and openbareruimtenaam is null and huisnummer is null and huisletter is null and huisnummertoevoeging is null and postcode is null and woonplaatsnaam is null)
group by kadastraalobject_id, kadastraalobject_volgnummer;
