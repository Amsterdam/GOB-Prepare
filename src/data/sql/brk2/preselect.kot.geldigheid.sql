SELECT kot.id,
       kot.volgnummer,
       kot.toestandsdatum AS begin_geldigheid,
       LEAD(kot.toestandsdatum, 1) OVER (
           PARTITION BY kot.identificatie
           ORDER BY kot.volgnummer
           )              AS eind_geldigheid
FROM brk2.kadastraal_object kot
         LEFT JOIN brk2.mutatie mte ON kot.mutatie_id = mte.id
WHERE kot.status_code = 'B'
