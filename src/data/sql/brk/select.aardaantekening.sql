-- Insert old aardaantekeing codes into the new table derived from kadaster
-- Default 'type' is 'Aantekening Object' -> always included
INSERT INTO brk.import_aardaantekening
    (
        select ca.code              as code,
               ca.omschrijving      as omschrijving,
               null                 as datum_vanaf,
               null                 as datum_tot,
               'Aantekening Object' as type
        from brk.c_aardaantekening ca
        where ca.code not in (select code from brk.import_aardaantekening)
    )
