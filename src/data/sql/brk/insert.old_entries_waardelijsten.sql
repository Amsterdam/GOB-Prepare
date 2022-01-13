-- Insert old aardaantekeing codes into the new table derived from kadaster
-- This table contains only the current codes, add the historical ones.
-- Default 'type' is 'Aantekening Object' -> always included
INSERT INTO brk.import_aardaantekening
    (
        SELECT ca.code              AS code,
               ca.omschrijving      AS omschrijving,
               NULL                 AS datum_vanaf,
               NULL                 AS datum_tot,
               'Aantekening Object' AS type
        FROM brk.c_aardaantekening ca
        WHERE ca.code NOT IN (SELECT code FROM brk.import_aardaantekening)
    );

-- Cultuur Bebouwd
INSERT INTO brk.import_cultuur_bebouwd
    (
        SELECT cbb.code              AS code,
               cbb.omschrijving      AS omschrijving,
               NULL                  AS datum_vanaf,
               NULL                  AS datum_tot,
               NULL                  AS type
        FROM brk.c_cultuurcodebebouwd cbb
        WHERE cbb.code NOT IN (SELECT icb.code FROM brk.import_cultuur_bebouwd icb)
    );

-- Cultuur Onbebouwd
INSERT INTO brk.import_cultuur_onbebouwd
    (
        SELECT cbo.code              AS code,
               cbo.omschrijving      AS omschrijving,
               NULL                  AS datum_vanaf,
               NULL                  AS datum_tot,
               NULL                  AS type
        FROM brk.c_cultuurcodeonbebouwd cbo
        WHERE cbo.code NOT IN (SELECT ico.code FROM brk.import_cultuur_onbebouwd ico)
    );
