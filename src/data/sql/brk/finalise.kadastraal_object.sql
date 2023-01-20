ALTER TABLE brk_prep.kadastraal_object RENAME TO kadastraal_object_old;

CREATE TABLE brk_prep.kadastraal_object AS
    SELECT
        kot.*,
        geo.geometrie
    FROM brk_prep.kadastraal_object_old kot
    LEFT JOIN brk_prep.kot_geo geo ON geo.nrn_kot_id = kot.nrn_kot_id AND geo.nrn_kot_volgnr = kot.nrn_kot_volgnr;