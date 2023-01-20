ALTER TABLE brk2_prep.kadastraal_object RENAME TO kadastraal_object_old;

CREATE TABLE brk2_prep.kadastraal_object AS
    SELECT
        kot.*,
        geo.geometrie
    FROM brk2_prep.kadastraal_object_old kot
    LEFT JOIN brk2_prep.kot_geo geo ON geo.id = kot.id AND geo.volgnummer = kot.volgnummer;