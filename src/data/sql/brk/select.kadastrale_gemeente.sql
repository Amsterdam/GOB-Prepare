-- use SnapToGrid to prevent precision errors
SELECT kad_gemeente ->> 'omschrijving'                      AS identificatie,
       ST_UNION(ST_SnapToGrid(geometrie, 0.000001))  AS geometrie,
       brg_gemeente ->> 'omschrijving'                      AS ligt_in_gemeente
FROM brk_prep.kadastraal_object
WHERE index_letter = 'G'
  AND ST_IsValid(geometrie)
  AND modification is NULL
  and status_code <> 'H'
GROUP BY kad_gemeente ->> 'omschrijving', brg_gemeente ->> 'omschrijving'
