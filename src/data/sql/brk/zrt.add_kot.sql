-- zakelijk_recht objects are linked through the zakelijkrecht_isbelastmet table.
--
-- The 'base' zakelijk_recht has a reference to kadastraal_object. The zakelijk_recht objects referencing the base
-- zakelijk_recht don't. This query places the references to kadastraal_object on all zakelijk_recht objects.
-- The query finds all references from the bottom up until no missing kadastraal_object references are left.
-- This query also sets the zrt_begindatum and zrt_einddatum as defined on kadastraal_object
CREATE OR REPLACE FUNCTION add_kot_to_zrt() RETURNS integer AS $$
DECLARE
  total integer := 0;
  lastres integer := 0;
  max_iter integer := 20;
  iter_cnt integer := 0;
BEGIN
  LOOP
  	UPDATE brk_prep.zakelijk_recht zrt
	SET
	    rust_op_kadastraalobject_id=kot_id,
	    rust_op_kadastraalobj_volgnr=kot_volgnr,
	    kadastraal_object_id=kot_identificatie,
	    kot_status_code=v.kot_status_code,
	    zrt_begindatum=begindatum,
	    zrt_einddatum=einddatum,
	    expiration_date=v.expiration_date,
	    toestandsdatum=v.toestandsdatum,
	    creation=v.creation,
	    modification=v.modification
	FROM (
		SELECT
		    zrtbelastmet.id,
		    zrtkot.rust_op_kadastraalobject_id,
		    zrtkot.rust_op_kadastraalobj_volgnr,
		    kot.brk_kot_id as kot_identificatie,
		    kot.status_code as kot_status_code,
		    kot.creation AS zrt_begindatum,
		    kot.einddatum as zrt_einddatum,
		    kot.expiration_date as expiration_date,
		    kot.toestandsdatum as toestandsdatum,
		    kot.creation as creation,
		    kot.modification as modification
		FROM brk.zakelijkrecht_isbelastmet bel
		LEFT JOIN brk_prep.zakelijk_recht zrtkot
		    ON zrtkot.id = zakelijkrecht_id
		LEFT JOIN brk_prep.zakelijk_recht zrtbelastmet
		    ON zrtbelastmet.id = bel.is_belast_met
		LEFT JOIN brk_prep.kadastraal_object kot
		    ON kot.nrn_kot_id = zrtkot.rust_op_kadastraalobject_id
	        AND kot.nrn_kot_volgnr = zrtkot.rust_op_kadastraalobj_volgnr
		WHERE zrtkot.rust_op_kadastraalobject_id IS NOT NULL
		    AND zrtbelastmet.rust_op_kadastraalobject_id IS NULL
	) AS v(id, kot_id, kot_volgnr, kot_identificatie, kot_status_code, begindatum, einddatum, expiration_date, toestandsdatum, creation, modification)
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
