-- Insert old aardaantekeing codes into the new table derived from kadaster
-- Default 'type' is null -> always included
INSERT INTO brk.import_aardaantekening
    (
        select ca.code          as code,
               ca.omschrijving  as omschrijving,
               null             as datum_vanaf,
               null             as datum_tot,
               null             as type
        from brk.c_aardaantekening ca
        where ca.code not in (select Code from brk.import_aardaantekening)
    )
