SELECT
          kad_gemeentecode->>'omschrijving'                       AS identificatie
         ,ST_UNION(geometrie)                                     AS geometrie
         ,kad_gemeente->>'omschrijving'                           AS is_onderdeel_van_kadastralegemeente
         FROM   brk_prep.kadastraal_object
         WHERE index_letter = 'G' AND ST_IsValid(geometrie) AND modification is NULL and status_code <> 'H'
         GROUP BY kad_gemeentecode->>'omschrijving', kad_gemeente->>'omschrijving'
