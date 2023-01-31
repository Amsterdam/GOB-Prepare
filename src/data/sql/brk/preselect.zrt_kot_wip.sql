CREATE TABLE brk_prep.zrt_kot_wip AS
SELECT zrt.identificatie,
       zrt.id,
       kot.nrn_kot_id                   AS rust_op_kadastraalobject_id,
       zrt.rust_op_kadastraalobj_volgnr AS rust_op_kadastraalobj_volgnr,
       kot.brk_kot_id                   AS kadastraal_object_id,
       kot.status_code                  AS kot_status_code,
       kot.creation                     AS zrt_begindatum,
       kot.modification                 AS zrt_einddatum,
       kot.expiration_date              AS expiration_date,
       kot.toestandsdatum               AS toestandsdatum,
       kot.creation                     AS creation,
       kot.modification                 AS modification
FROM brk.zakelijkrecht zrt
         LEFT JOIN brk_prep.kadastraal_object kot
                   ON zrt.rust_op_kadastraalobject_id = kot.nrn_kot_id AND
                      zrt.rust_op_kadastraalobj_volgnr = kot.nrn_kot_volgnr;

-- zakelijk_recht objects are linked through the zakelijkrecht_isbelastmet table.
--
-- The 'base' zakelijk_recht has a reference to kadastraal_object. The zakelijk_recht objects referencing the base
-- zakelijk_recht don't. This query places the references to kadastraal_object on all zakelijk_recht objects.
-- The query finds all references from the bottom up until no missing kadastraal_object references are left.
-- This query also sets the zrt_begindatum and zrt_einddatum as defined on kadastraal_object
CREATE OR REPLACE FUNCTION add_kot_to_zrt() RETURNS integer AS
$$
DECLARE
    total    integer := 0;
    lastres  integer := 0;
    max_iter integer := 20;
    iter_cnt integer := 0;
BEGIN
    CREATE TEMPORARY TABLE isbelastmet
    (
        id                           int,
        rust_op_kadastraalobject_id  int,
        rust_op_kadastraalobj_volgnr int,
        kot_identificatie            varchar,
        kot_status_code              varchar,
        zrt_begindatum               timestamp,
        zrt_einddatum                timestamp,
        expiration_date              timestamp,
        teostandsdatum               timestamp,
        creation                     timestamp,
        modification                 timestamp
    );

    LOOP
        INSERT INTO isbelastmet
        SELECT zrtbelastmet.id,
               zrtkot.rust_op_kadastraalobject_id,
               zrtkot.rust_op_kadastraalobj_volgnr,
               kot.brk_kot_id      AS kot_identificatie,
               kot.status_code     AS kot_status_code,
               kot.creation        AS zrt_begindatum,
               kot.einddatum       AS zrt_einddatum,
               kot.expiration_date AS expiration_date,
               kot.toestandsdatum  AS toestandsdatum,
               kot.creation        AS creation,
               kot.modification    AS modification
        FROM brk.zakelijkrecht_isbelastmet bel
                 LEFT JOIN brk_prep.zrt_kot_wip zrtkot
                           ON zrtkot.id = zakelijkrecht_id
                 LEFT JOIN brk_prep.zrt_kot_wip zrtbelastmet
                           ON zrtbelastmet.id = bel.is_belast_met
                 LEFT JOIN brk_prep.kadastraal_object kot
                           ON kot.nrn_kot_id = zrtkot.rust_op_kadastraalobject_id
                               AND kot.nrn_kot_volgnr = zrtkot.rust_op_kadastraalobj_volgnr
        WHERE zrtkot.rust_op_kadastraalobject_id IS NOT NULL
          AND zrtbelastmet.rust_op_kadastraalobject_id IS NULL;

        UPDATE brk_prep.zrt_kot_wip zrt
        SET rust_op_kadastraalobject_id=kot_id,
            rust_op_kadastraalobj_volgnr=kot_volgnr,
            kadastraal_object_id=kot_identificatie,
            kot_status_code=v.kot_status_code,
            zrt_begindatum=begindatum,
            zrt_einddatum=einddatum,
            expiration_date=v.expiration_date,
            toestandsdatum=v.toestandsdatum,
            creation=v.creation,
            modification=v.modification
        FROM isbelastmet AS v(id, kot_id, kot_volgnr, kot_identificatie,
                              kot_status_code, begindatum, einddatum,
                              expiration_date, toestandsdatum, creation,
                              modification)
        WHERE v.id = zrt.id;

        GET DIAGNOSTICS lastres = ROW_COUNT;
        total := total + lastres;
        iter_cnt := iter_cnt + 1;

        TRUNCATE isbelastmet;

        EXIT WHEN lastres = 0 OR iter_cnt > max_iter;
    END LOOP;
    RETURN total;
END;
$$ LANGUAGE plpgsql;
SELECT add_kot_to_zrt();
