SELECT id,
       volgnummer,
       begin_geldigheid,
       eind_geldigheid
FROM (SELECT kot.id,
             kot.volgnummer,
             kot.toestandsdatum AS begin_geldigheid,
             -- kot.toestandsdatum is the date as from BRK
             -- However, in Neuron, a deletion triggers the creation of a new volgnummer with status_code = 'H'. This
             -- volgnummer has the same toestandsdatum as the previous volgnummer, as the 'wordt' of the BRK Monitor
             -- message is empty. The deletion date would in that case be the mutation date. That is why, for every
             -- volgnummer, we take the toestandsdatum of the next volgnummer as the eind_geldigheid, but in case the
             -- next volgnummer is of status 'H', we take the mutation date.
             LEAD(CASE WHEN kot.status_code = 'B' THEN kot.toestandsdatum ELSE mte.datum END, 1) OVER (
                 PARTITION BY kot.identificatie
                 ORDER BY kot.volgnummer
                 )              AS eind_geldigheid,
             kot.status_code
      FROM brk2.kadastraal_object kot
               LEFT JOIN brk2.mutatie mte ON kot.mutatie_id = mte.id) q
WHERE q.status_code = 'B'
