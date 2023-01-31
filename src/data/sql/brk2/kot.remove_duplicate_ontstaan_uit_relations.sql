CREATE OR REPLACE FUNCTION brk2_undouble_g_perceel_relations() RETURNS integer AS
$$
    -- Removes duplicate relations from is_ontstaan_uit_brk_g_perceel and is_ontstaan_uit_brk_kadastraalobject columns.
-- JSON objects in those columns contain (id, volgnummer, identificatie) combinations because they are
-- needed in preselect.kot_geo.sql. We remove the id and volgnummer attributes from the JSON and undouble
-- the resulting list of objects with only the 'identificatie' attribute.
DECLARE
    batch_size integer := 10000;
    max_id     integer;
    current_id integer := 0;
    total      integer := 0;
    lastres    integer := 0;
BEGIN
    SELECT MAX(id) FROM brk2_prep.kadastraal_object INTO max_id;
    CREATE TABLE brk2_prep.ontstaan_uit_g_perceel
    (
        id           int,
        volgnummer   int,
        new_relation jsonb
    );
    CREATE TABLE brk2_prep.ontstaan_uit_kadastraalobject
    (
        id           int,
        volgnummer   int,
        new_relation jsonb
    );

    LOOP
        -- Undouble g_perceel
        INSERT INTO brk2_prep.ontstaan_uit_g_perceel
        SELECT id,
               volgnummer,
               ARRAY_TO_JSON(
                       ARRAY_AGG(
                               JSON_BUILD_OBJECT(
                                       'kot_identificatie', kot_identificatie
                                   )
                           )
                   ) AS new_relation
        FROM (SELECT kot.id,
                     kot.volgnummer,
                     gperc ->> 'kot_identificatie' AS kot_identificatie
              FROM brk2_prep.kadastraal_object kot
                       JOIN JSONB_ARRAY_ELEMENTS(kot.is_ontstaan_uit_brk_g_perceel) gperc ON TRUE
              WHERE kot.is_ontstaan_uit_brk_g_perceel IS NOT NULL
                AND kot.id >= current_id
                AND kot.id < (current_id + batch_size)
              GROUP BY kot.id, kot.volgnummer, gperc ->> 'kot_identificatie') q
        GROUP BY id, volgnummer;

        -- Undouble ontstaan_uit_kadastraalobject
        INSERT INTO brk2_prep.ontstaan_uit_kadastraalobject
        SELECT id,
               volgnummer,
               ARRAY_TO_JSON(
                       ARRAY_AGG(
                               JSON_BUILD_OBJECT(
                                       'kot_identificatie', kot_identificatie
                                   )
                           )
                   ) AS new_relation
        FROM (SELECT kot.id,
                     kot.volgnummer,
                     gperc ->> 'kot_identificatie' AS kot_identificatie
              FROM brk2_prep.kadastraal_object kot
                       JOIN JSONB_ARRAY_ELEMENTS(kot.is_ontstaan_uit_brk_kadastraalobject) gperc ON TRUE
              WHERE kot.is_ontstaan_uit_brk_kadastraalobject IS NOT NULL
                AND kot.id >= current_id
                AND kot.id < (current_id + batch_size)
              GROUP BY kot.id, kot.volgnummer, gperc ->> 'kot_identificatie') q
        GROUP BY id, volgnummer;

        current_id = current_id + batch_size;
        EXIT WHEN current_id > max_id;
    END LOOP;

    -- Update g_perceel relations
    UPDATE brk2_prep.kadastraal_object kot
    SET is_ontstaan_uit_brk_g_perceel=t.new_relation
    FROM brk2_prep.ontstaan_uit_g_perceel t
    WHERE t.id = kot.id
      AND t.volgnummer = kot.volgnummer;

    GET DIAGNOSTICS lastres = ROW_COUNT;
    total = total + lastres;

    -- Update ontstaan-uit_kadastraalobject relations
    UPDATE brk2_prep.kadastraal_object kot
    SET is_ontstaan_uit_brk_kadastraalobject=t.new_relation
    FROM brk2_prep.ontstaan_uit_kadastraalobject t
    WHERE t.id = kot.id
      AND t.volgnummer = kot.volgnummer;

    GET DIAGNOSTICS lastres = ROW_COUNT;
    total = total + lastres;

    RETURN total;

END;
$$ LANGUAGE plpgsql;

SELECT brk2_undouble_g_perceel_relations();
