SELECT aangeduid_door_brk_kadastralegemeentecode_omschrijving || aangeduid_door_brk_kadastralesectie AS identificatie,
       aangeduid_door_brk_kadastralesectie                                                           AS code,
       aangeduid_door_brk_kadastralegemeentecode_omschrijving                                        AS is_onderdeel_van_brk_kadastrale_gemeentecode,
       ST_Union(ST_SnapToGrid(geometrie, 0.0001))                                                    AS geometrie
FROM brk2_prep.kadastraal_object
WHERE indexletter = 'G'
  AND ST_IsValid(geometrie)
  AND _expiration_date IS NULL
GROUP BY aangeduid_door_brk_kadastralegemeentecode_omschrijving, aangeduid_door_brk_kadastralesectie
