ALTER TABLE brk2_prep.kadastraal_object RENAME TO kadastraal_object_old;

CREATE TABLE brk2_prep.kadastraal_object AS
    SELECT
        kot.*,
        geo.geometrie,
        ontst_uit_kot.is_ontstaan_uit_brk_kadastraalobject::jsonb,
        ontst_uit_g.is_ontstaan_uit_brk_g_perceel::jsonb
    FROM brk2_prep.kadastraal_object_old kot
    LEFT JOIN brk2_prep.kot_geo geo ON geo.id = kot.id AND geo.volgnummer = kot.volgnummer
    LEFT JOIN brk2_prep.kot_ontstaan_uit_kot ontst_uit_kot ON kot.id = ontst_uit_kot.kot_id AND kot.volgnummer = ontst_uit_kot.kot_volgnummer
    LEFT JOIN brk2_prep.kot_ontstaan_uit_g_perceel ontst_uit_g ON kot.id = ontst_uit_g.kot_id AND kot.volgnummer = ontst_uit_g.kot_volgnummer
;
