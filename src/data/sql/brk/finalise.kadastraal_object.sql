ALTER TABLE brk_prep.kadastraal_object RENAME TO kadastraal_object_old;

CREATE TABLE brk_prep.kadastraal_object AS
    SELECT
        kot.*,
        geo.geometrie,
        ontst_uit_kot.ontstaan_uit_kadastraalobject::jsonb,
        ontst_uit_g.relatie_g_perceel::jsonb
    FROM brk_prep.kadastraal_object_old kot
    LEFT JOIN brk_prep.kot_geo geo ON geo.nrn_kot_id = kot.nrn_kot_id AND geo.nrn_kot_volgnr = kot.nrn_kot_volgnr
    LEFT JOIN brk_prep.kot_ontstaan_uit_kot ontst_uit_kot ON ontst_uit_kot.nrn_kot_id = kot.nrn_kot_id AND ontst_uit_kot.nrn_kot_volgnr = kot.nrn_kot_volgnr
    LEFT JOIN brk_prep.kot_ontstaan_uit_g_perceel ontst_uit_g ON ontst_uit_g.nrn_kot_id = kot.nrn_kot_id AND ontst_uit_g.nrn_kot_volgnr = kot.nrn_kot_volgnr
;