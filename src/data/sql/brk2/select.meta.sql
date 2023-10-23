SELECT 1                                                                                                               AS id,
       DATE_TRUNC('DAY', MAX(bsd.datum_aangemaakt))                                                                    AS toestandsdatum,
       'Toestandsdatum, d.i. de laatste aanmaakdatum van de BRK-berichten in de naar DIVA gerepliceerde BRK-levering.' AS omschrijving
FROM brk2.bestand bsd
;
