-- Reference betrokken_bij
UPDATE brk_prep.zakelijk_recht zrt
SET betrokken_bij= (
	SELECT array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie)))
	FROM (
		SELECT identificatie
		FROM brk_prep.zakelijk_recht zrt2
		WHERE zrt2.ontstaan_uit_asg_id=zrt.betrokken_bij_asg_id
		GROUP BY identificatie
	) q
)
;

-- Reference ontstaan_uit
UPDATE brk_prep.zakelijk_recht zrt
SET ontstaan_uit= (
	SELECT
		array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie)))
	FROM (
		SELECT identificatie
		FROM brk_prep.zakelijk_recht zrt2
		WHERE zrt2.betrokken_bij_asg_id=zrt.ontstaan_uit_asg_id
		GROUP BY identificatie
	) q
)
;