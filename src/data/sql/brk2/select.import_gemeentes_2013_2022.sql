CREATE TABLE brk2_prep.import_gemeentes_2013_2022 AS
SELECT
    identificatie,
    volgnummer,
    naam,
    begin_geldigheid,
    eind_geldigheid,
    geometrie
FROM brk2.import_gemeentes_2013_2022
;
