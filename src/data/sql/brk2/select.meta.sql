CREATE TABLE brk2_prep.meta AS
SELECT 1                                                                                                        AS id,
       DATE_TRUNC('DAY', MAX(bsd.datum_aangemaakt))                                                             AS toestandsdatum,
       'Toestandsdatum, d.i. de laatste aanmaakdatum van de BRK-berichten in de laatst verwerkte BRK-levering.' AS omschrijving
FROM brk2.bestand bsd
;
