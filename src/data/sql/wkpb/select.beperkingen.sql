SELECT
  b1.registernummer || p1.belast_kadastraal_object AS id,
  b1.registernummer AS src_id,
  b1.volgnummer AS src_volgnummer,
  p1.belast_kadastraal_object AS bronwaarde
FROM (
  SELECT
  b.id
  , b.registernummer
  , b.volgnummer
  FROM prb.beperking b
  -- select max volgnummer van iedere beperking
  LEFT JOIN (
    SELECT id, MAX(volgnummer) AS volgnummer FROM prb.beperking GROUP BY id
  ) m ON m.id = b.id AND m.volgnummer = b.volgnummer
  WHERE b.registernummer IS NOT NULL
  AND m.id IS NOT NULL
) b1
LEFT OUTER JOIN (
  SELECT
    p.id_beperking,
    p.begin_volgnummer,
    p.einde_volgnummer,
    p.gemcod || p.sectie || LPAD(p.pnum, 5, '0') || p.objindl || LPAD(p.objindn, 4, '0') AS belast_kadastraal_object
  FROM prb.beperking_perceel p
) p1 ON b1.id = p1.id_beperking
AND b1.volgnummer BETWEEN p1.begin_volgnummer AND CASE p1.einde_volgnummer WHEN 0 THEN 999999 ELSE p1.einde_volgnummer END
WHERE p1.belast_kadastraal_object IS NOT NULL