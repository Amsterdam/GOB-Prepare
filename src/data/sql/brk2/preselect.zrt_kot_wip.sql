CREATE TABLE brk2_prep.zrt_kot_wip AS SELECT zrt.identificatie,
                                         zrt.id,
                                         zrt.rust_op_kadastraalobj_volgnr AS volgnummer,
                                         kot.identificatie     AS rust_op_kadastraalobject,
                                         kot.id                AS __rust_op_kot_id,
                                         kot.volgnummer        AS __rust_op_kot_volgnummer,
                                         kot.begin_geldigheid  AS begin_geldigheid,
                                         kot.eind_geldigheid   AS eind_geldigheid,
                                         kot.toestandsdatum    AS toestandsdatum,
                                         kot.datum_actueel_tot AS datum_actueel_tot,
                                         kot._expiration_date  AS _expiration_date
                                  FROM brk2.zakelijkrecht zrt
                                           LEFT JOIN brk2_prep.kadastraal_object kot
                                                     ON zrt.rust_op_kadastraalobject_id = kot.id AND zrt.rust_op_kadastraalobj_volgnr = kot.volgnummer;

-- zakelijk_recht objects are linked through the zakelijkrecht_isbelastmet table.
--
-- The 'base' zakelijk_recht has a reference to kadastraal_object. The zakelijk_recht objects referencing the base
-- zakelijk_recht don't. This query places the references to kadastraal_object on all zakelijk_recht objects.
-- The query finds all references from the bottom up until no missing kadastraal_object references are left.
-- This query also sets the dates on the zrt objects that are derived from the underlying kot.
CREATE OR REPLACE FUNCTION add_kot_to_zrt() RETURNS integer AS $$
DECLARE
    total integer := 0;
    lastres integer := 0;
    max_iter integer := 20;
    iter_cnt integer := 0;
BEGIN
    LOOP
        UPDATE brk2_prep.zrt_kot_wip zrt
        SET
            __rust_op_kot_id=kot_id,
            __rust_op_kot_volgnummer=v.volgnummer,
            volgnummer=v.volgnummer,
            rust_op_kadastraalobject=kot_identificatie,
            toestandsdatum=v.toestandsdatum,
            begin_geldigheid=v.begin_geldigheid,
            eind_geldigheid=v.eind_geldigheid,
            _expiration_date=v._expiration_date,
            datum_actueel_tot=v.datum_actueel_tot
        FROM (
                 SELECT
                     zrtbelastmet.id,
                     zrtkot.__rust_op_kot_id AS kot_id,
                     zrtkot.__rust_op_kot_volgnummer AS volgnummer,
                     kot.identificatie AS kot_identificatie,
                     kot.toestandsdatum,
                     kot.begin_geldigheid,
                     kot.eind_geldigheid,
                     kot._expiration_date,
                     kot.datum_actueel_tot
                 FROM brk2.zakelijkrecht_isbelastmet bel
                          LEFT JOIN brk2_prep.zrt_kot_wip zrtkot
                                    ON zrtkot.id = zakelijkrecht_id
                          LEFT JOIN brk2_prep.zrt_kot_wip zrtbelastmet
                                    ON zrtbelastmet.id = bel.isbelastmet_id
                          LEFT JOIN brk2_prep.kadastraal_object kot
                                    ON kot.id = zrtkot.__rust_op_kot_id
                                        AND kot.volgnummer = zrtkot.__rust_op_kot_volgnummer
                 WHERE zrtkot.__rust_op_kot_id IS NOT NULL
                   AND zrtbelastmet.__rust_op_kot_volgnummer IS NULL
             ) AS v(
                    id,
                    kot_id,
                    volgnummer,
                    kot_identificatie,
                    toestandsdatum,
                    begin_geldigheid,
                    eind_geldigheid,
                    _expiration_date,
                    datum_actueel_tot
            )
        WHERE v.id = zrt.id;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total := total + lastres;
        iter_cnt := iter_cnt + 1;

        EXIT WHEN lastres = 0 OR iter_cnt > max_iter;
    END LOOP;
    RETURN total;
END;
$$ LANGUAGE plpgsql;
SELECT add_kot_to_zrt();
