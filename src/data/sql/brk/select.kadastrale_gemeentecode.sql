SELECT
          kad_gemeentecode->>'omschrijving'                       AS identificatie
         ,ST_UNION(geometrie)                                     AS geometrie
         ,kad_gemeente->>'omschrijving'                           AS is_onderdeel_van_kadastralegemeente
         FROM   brk_prep.kadastraal_object
         WHERE index_letter = 'G'
         GROUP BY kad_gemeentecode->>'omschrijving'
