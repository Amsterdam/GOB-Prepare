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
  	UPDATE brk2_prep.zakelijk_recht zrt
	SET
	    __rust_op_kot_id=kot_id,
	    __rust_op_kot_volgnummer=v.volgnummer,
	    rust_op_kadastraalobject=kot_identificatie,
	    toestandsdatum=v.toestandsdatum,
	    begin_geldigheid=v.begin_geldigheid,
	    eind_geldigheid=v.eind_geldigheid,
	    _expiration_date=v._expiration_date,
	    datum_actueel_tot=v.datum_actueel_tot
	FROM (
		SELECT
		    zrtbelastmet.__id,
		    zrtkot.__rust_op_kot_id AS kot_id,
		    zrtkot.__rust_op_kot_volgnummer AS volgnummer,
		    kot.identificatie AS kot_identificatie,
		    kot.toestandsdatum,
		    kot.begin_geldigheid,
		    kot.eind_geldigheid,
		    kot._expiration_date,
		    kot.datum_actueel_tot
		FROM brk2.zakelijkrecht_isbelastmet bel
		LEFT JOIN brk2_prep.zakelijk_recht zrtkot
		    ON zrtkot.__id = zakelijkrecht_id
		LEFT JOIN brk2_prep.zakelijk_recht zrtbelastmet
		    ON zrtbelastmet.__id = bel.isbelastmet_id
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
	WHERE v.id = zrt.__id;

    GET DIAGNOSTICS lastres = ROW_COUNT;
  	total := total + lastres;
  	iter_cnt := iter_cnt + 1;

  	EXIT WHEN lastres = 0 OR iter_cnt > max_iter;
  END LOOP;
  RETURN total;
END;
$$ LANGUAGE plpgsql;
SELECT add_kot_to_zrt();
