CREATE TABLE brk2_prep.meta AS
SELECT
    1 as id,
    bsd.brk_bsd_toestandsdatum as toestandsdatum,
    bsd.omschrijving
FROM brk2.bestand bsd
