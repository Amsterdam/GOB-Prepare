SELECT
          kad_gemeentecode->>'omschrijving' || sectie             AS identificatie
         ,sectie                                                  AS code
         ,ST_UNION(geometrie)                                     AS geometrie
         ,kad_gemeentecode->>'omschrijving'                       AS is_onderdeel_van_kadastralegemeentecode
         FROM   brk_prep.kadastraal_object
         WHERE index_letter = 'G'
         GROUP BY kad_gemeentecode->>'omschrijving', sectie
