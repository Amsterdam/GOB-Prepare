SELECT	aangeduid_door_kadastralegemeentecode_omschrijving  || aangeduid_door_kadastralesectie	AS identificatie,
		aangeduid_door_kadastralesectie															AS code,
		ST_UNION(ST_SnapToGrid(geometrie, 0.0001))												AS geometrie,
		aangeduid_door_kadastralegemeentecode_omschrijving 										AS is_onderdeel_van_kadastralegemeentecode
FROM brk2_prepared.kadastraal_object
WHERE indexletter = 'G'
	AND ST_IsValid(geometrie)
	AND datum_actueel_tot IS null
GROUP BY aangeduid_door_kadastralegemeentecode_omschrijving ,aangeduid_door_kadastralesectie