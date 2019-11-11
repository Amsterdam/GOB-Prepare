CREATE MATERIALIZED VIEW brk_prep.subject_expiration_date AS (
    SELECT
    	q.subject_id,
    	CASE WHEN sum(CASE WHEN expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(expiration_date) END AS expiration_date
    FROM (
             SELECT
             	tng.van_subject_id,
             	CASE WHEN sum(CASE WHEN tng.einddatum IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(tng.einddatum) END AS expiration_date
             FROM brk_prep.tenaamstelling tng
             GROUP BY tng.van_subject_id
             --
             UNION
             --
             SELECT
                zrt.betrokken_bij_ref,
                NULL AS expiration_date
             FROM brk_prep.zakelijk_recht zrt
             WHERE zrt.betrokken_bij_ref IS NOT NULL
             GROUP BY zrt.betrokken_bij_ref
			 --
             UNION
  			 --
             SELECT
             	sjt_id.obj ->> 'brk_sjt_id',
             	CASE WHEN sum(CASE WHEN akt.expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(akt.expiration_date) END AS expiration_date
             FROM brk_prep.aantekening_kadastraal_object akt
             JOIN jsonb_array_elements(akt.brk_sjt_ids) sjt_id(obj) ON TRUE
             WHERE akt.brk_sjt_ids <> 'null'
             GROUP BY sjt_id.obj->>'brk_sjt_id'
    		 --
             UNION
      		 --
             SELECT
             	sjt_id.obj ->> 'brk_sjt_id',
             	CASE WHEN sum(CASE WHEN art.expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(art.expiration_date) END AS expiration_date
             FROM brk_prep.aantekening_recht art
             JOIN jsonb_array_elements(art.brk_sjt_ids) sjt_id(obj) ON TRUE
             WHERE art.brk_sjt_ids <> 'null'
             GROUP BY sjt_id.obj->>'brk_sjt_id'
         ) q(subject_id, expiration_date)
    GROUP BY q.subject_id
);
