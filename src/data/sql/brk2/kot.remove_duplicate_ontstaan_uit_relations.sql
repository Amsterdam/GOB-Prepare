CREATE OR REPLACE FUNCTION brk2_undouble_g_perceel_relations() RETURNS integer AS
$$
    -- Removes duplicate relations from is_ontstaan_uit_g_perceel and is_ontstaan_uit_kadastraalobject columns.
-- JSON objects in those columns contain (id, volgnummer, identificatie) combinations because they are
-- needed in kot.update_a_geometrie.sql. We remove the id and volgnummer attributes from the JSON and undouble
-- the resulting list of objects with only the 'identificatie' attribute.
DECLARE
    batch_size integer := 10000;
    max_id     integer;
    current_id integer := 0;
    total      integer := 0;
    lastres    integer := 0;
BEGIN
    SELECT max(id) FROM brk2_prep.kadastraal_object INTO max_id;
    LOOP
        -- Undouble g_perceel
        UPDATE brk2_prep.kadastraal_object kot
        SET is_ontstaan_uit_g_perceel=new_relations.new_relation
        FROM (SELECT id,
                     volgnummer,
                     array_to_json(
                             array_agg(
                                     json_build_object(
                                             'kot_identificatie', kot_identificatie
                                         )
                                 )
                         ) as new_relation
              FROM (SELECT kot.id,
                           kot.volgnummer,
                           gperc ->> 'kot_identificatie' AS kot_identificatie
                    FROM brk2_prep.kadastraal_object kot
                             JOIN jsonb_array_elements(kot.is_ontstaan_uit_g_perceel) gperc ON TRUE
                    WHERE kot.is_ontstaan_uit_g_perceel IS NOT NULL
                      AND kot.id >= current_id
                      AND kot.id < (current_id + batch_size)
                    GROUP BY kot.id, kot.volgnummer, gperc ->> 'kot_identificatie') q
              GROUP BY id, volgnummer) new_relations
        WHERE new_relations.id = kot.id
          AND new_relations.volgnummer = kot.volgnummer;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total = total + lastres;

        -- Undouble ontstaan_uit_kadastraalobject
        UPDATE brk2_prep.kadastraal_object kot
        SET is_ontstaan_uit_kadastraalobject=new_relations.new_relation
        FROM (SELECT id,
                     volgnummer,
                     array_to_json(
                             array_agg(
                                     json_build_object(
                                             'kot_identificatie', kot_identificatie
                                         )
                                 )
                         ) as new_relation
              FROM (SELECT kot.id,
                           kot.volgnummer,
                           gperc ->> 'kot_identificatie' AS kot_identificatie
                    FROM brk2_prep.kadastraal_object kot
                             JOIN jsonb_array_elements(kot.is_ontstaan_uit_kadastraalobject) gperc ON TRUE
                    WHERE kot.is_ontstaan_uit_kadastraalobject IS NOT NULL
                      AND kot.id >= current_id
                      AND kot.id < (current_id + batch_size)
                    GROUP BY kot.id, kot.volgnummer, gperc ->> 'kot_identificatie') q
              GROUP BY id, volgnummer) new_relations
        WHERE new_relations.id = kot.id
          AND new_relations.volgnummer = kot.volgnummer;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total = total + lastres;

        current_id = current_id + batch_size;
        EXIT WHEN current_id > max_id;
    END LOOP;
    RETURN total;

END;
$$ LANGUAGE plpgsql;

SELECT brk2_undouble_g_perceel_relations();
