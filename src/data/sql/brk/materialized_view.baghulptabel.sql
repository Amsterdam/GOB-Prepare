create materialized view brk.baghulptabel as select
	kadastraalobject_id,
	kadastraalobject_volgnummer,
	
	array_to_json(array_agg(json_build_object(
	    'koppelingswijze_code', koppelingswijze_code,
		'koppelingswijze_omschrijving', koppelingswijze_omschrijving,
		'bag_id', bag_id,
	    'openbareruimtenaam', openbareruimtenaam,
	    'huisnummer', huisnummer,
	    'huisletter', huisletter,
	    'huisnummertoevoeging', huisnummertoevoeging,
	    'postcode', postcode,
	    'woonplaatsnaam', woonplaatsnaam
		'woonplaatsnaam_afwijkend', woonplaatsnaam_afwijkend
		'hoofdadres_identificatie', hoofdadres_identificatie
		'typering', typering)
	    ORDER BY
	        koppelingswijze_code,
			koppelingswijze_omschrijving,
			bag_id,
	        openbareruimtenaam,
	        huisnummer,
	        huisletter,
	        huisnummertoevoeging,
	        postcode,
	        woonplaatsnaam,
			woonplaatsnaam_afwijkend,
			hoofdadres_identificatie,
			typering)
	    ) as adressen
from (
SELECT kol.kadastraalobject_id			
	  ,kol.kadastraalobject_volgnummer			
	  ,kot.kadastraalobject_identificatie 			 --toegevoegd
	  ,kol.koppelingswijze_code           			 --toegevoegd
	  ,ckw.omschrijving AS koppelingswijze_omschrijving  --toegevoegd
	  ,obl.bag_identificatie AS bag_id    			 --adresseerbaar object id
	  ,obl.openbareruimtenaam			
	  ,obl.huisnummer			
	  ,obl.huisletter			
	  ,obl.huisnummertoevoeging			
	  ,obl.postcode			
	  ,obl.woonplaatsnaam			
	  ,obl.woonplaatsnaam_afwijkend       			 --toegevoegd, onduidelijk wat het is, nu nog leeg, kan misschien later een rol spelen
	  ,aol.hoofdadres_identificatie       			 --toegevoegd, nag-id
	  ,aol.typering                       			 --toegevoegd; type aot, dus vot,lps of sps
FROM   brk2.kadastraal_object_locatie kol
LEFT JOIN c_koppelingswijze ckw 
ON     (kol.koppelingswijze_code=ckw.code)
LEFT JOIN kadastraal_object kot
ON     (kol.kadastraalobject_id=kot.id AND kol.kadastraalobject_volgnummer=kot.volgnummer)
LEFT JOIN objectlocatie_binnenland  obl
ON     kol.locatie_identificatie=obl.identificatie
LEFT JOIN adresseerbaar_object aol
ON     obl.bag_identificatie=aol.bag_identificatie   --er komen ook nevenadressen voor, maar die zijn mijns inziens overbodig, dus niet toegevoegd.
);