CREATE OR REPLACE FUNCTION add_kot_to_zrt() RETURNS integer AS $$
DECLARE
  total integer := 0;
  lastres integer := 0;
  max_iter integer := 20;
  iter_cnt integer := 0;
BEGIN
  LOOP
  	UPDATE brk_prep.zakelijk_recht zrt
	SET rust_op_kadastraalobject_id=kot_id, rust_op_kadastraalobj_volgnr=kot_volgnr
	FROM (
		SELECT zrtbelastmet.id, zrtkot.rust_op_kadastraalobject_id, zrtkot.rust_op_kadastraalobj_volgnr
		FROM brk.zakelijkrecht_isbelastmet bel
		LEFT JOIN brk_prep.zakelijk_recht zrtkot
		ON zrtkot.id = zakelijkrecht_id
		LEFT JOIN brk_prep.zakelijk_recht zrtbelastmet
		ON zrtbelastmet.id = bel.is_belast_met
		WHERE zrtkot.rust_op_kadastraalobject_id IS NOT NULL
		AND zrtbelastmet.rust_op_kadastraalobject_id IS NULL
	) AS v(id, kot_id, kot_volgnr)
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