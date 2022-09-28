CREATE MATERIALIZED VIEW brk2_prep.subject_expiration_date AS (
    SELECT
    	q.subject_id,
    	CASE WHEN sum(CASE WHEN expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(expiration_date) END AS expiration_date
    FROM (
             SELECT tng.van_brk_kadastraalsubject,
                    CASE
                        WHEN sum(CASE WHEN tng._expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL
                        ELSE max(tng._expiration_date) END AS expiration_date
             FROM brk2_prep.tenaamstelling tng
             GROUP BY tng.van_brk_kadastraalsubject
             --
             UNION
             --
             SELECT
                zrt.betrokken_bij_appartementsrechtsplitsing_vve,
                NULL AS expiration_date
             FROM brk2_prep.zakelijk_recht zrt
             WHERE zrt.betrokken_bij_appartementsrechtsplitsing_vve IS NOT NULL
             GROUP BY zrt.betrokken_bij_appartementsrechtsplitsing_vve

-- 			 --
--              UNION
--   			 --
--              SELECT
--              	sjt_id.obj ->> 'brk_sjt_id',
--              	CASE WHEN sum(CASE WHEN akt.expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(akt.expiration_date) END AS expiration_date
--              FROM brk2_prep.aantekening_kadastraal_object akt
--              JOIN jsonb_array_elements(akt.brk_sjt_ids) sjt_id(obj) ON TRUE
--              WHERE akt.brk_sjt_ids <> 'null'
--              GROUP BY sjt_id.obj->>'brk_sjt_id'
--     		 --
--              UNION
--       		 --
--              SELECT
--              	sjt_id.obj ->> 'brk_sjt_id',
--              	CASE WHEN sum(CASE WHEN art.expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL ELSE max(art.expiration_date) END AS expiration_date
--              FROM brk2_prep.aantekening_recht art
--              JOIN jsonb_array_elements(art.brk_sjt_ids) sjt_id(obj) ON TRUE
--              WHERE art.brk_sjt_ids <> 'null'
--              GROUP BY sjt_id.obj->>'brk_sjt_id'
         ) q(subject_id, expiration_date)
    GROUP BY q.subject_id
);
