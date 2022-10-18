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
             SELECT zrt.betrokken_bij_appartementsrechtsplitsing_vve,
                    NULL AS expiration_date
             FROM brk2_prep.zakelijk_recht zrt
             WHERE zrt.betrokken_bij_appartementsrechtsplitsing_vve IS NOT NULL
             GROUP BY zrt.betrokken_bij_appartementsrechtsplitsing_vve
             --
             UNION
             --
             SELECT sjt.obj ->> 'sjt_identificatie',
                    CASE
                        WHEN SUM(CASE WHEN akt._expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL
                        ELSE MAX(akt._expiration_date) END AS expiration_date
             FROM brk2_prep.aantekening_kadastraal_object akt
                      JOIN JSONB_ARRAY_ELEMENTS(akt.heeft_brk_betrokken_persoon) sjt(obj) ON TRUE
             WHERE akt.heeft_brk_betrokken_persoon IS NOT NULL
             GROUP BY sjt.obj ->> 'sjt_identificatie'
             --
             UNION
             --
             SELECT sjt.obj ->> 'sjt_identificatie',
                    CASE
                        WHEN SUM(CASE WHEN art._expiration_date IS NULL THEN 1 ELSE 0 END) > 0 THEN NULL
                        ELSE MAX(art._expiration_date) END AS expiration_date
             FROM brk2_prep.aantekening_recht art
                      JOIN JSONB_ARRAY_ELEMENTS(art.heeft_brk_betrokken_persoon) sjt(obj) ON TRUE
             WHERE art.heeft_brk_betrokken_persoon IS NOT NULL
             GROUP BY sjt.obj ->> 'sjt_identificatie'
         ) q(subject_id, expiration_date)
    GROUP BY q.subject_id
);
