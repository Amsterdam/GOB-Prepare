SELECT
          kad_gemeentecode->>'omschrijving' || sectie             AS identificatie
         ,sectie                                                  AS code
         ,ST_UNION(geometrie)                                     AS geometrie
         ,kad_gemeentecode->>'omschrijving'                       AS is_onderdeel_van_kadastralegemeentecode
         FROM   brk_prep.kadastraal_object
         WHERE index_letter = 'G' AND ST_IsValid(geometrie) AND modification is NULL and status_code <> 'H'
         GROUP BY kad_gemeentecode->>'omschrijving', sectie
