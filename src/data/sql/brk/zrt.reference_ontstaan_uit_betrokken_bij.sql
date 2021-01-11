-- Reference betrokken_bij and determine max_zrt_begindatum
UPDATE brk_prep.zakelijk_recht zrt
SET betrokken_bij=q2.betrokken_bij,
    max_betrokken_bij_begindatum=q2.max_zrt_begindatum
FROM (
    SELECT
             array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) betrokken_bij,
             max(zrt_begindatum) max_zrt_begindatum,
             ontstaan_uit_asg_id
         FROM (
                  SELECT identificatie,
                         zrt2.ontstaan_uit_asg_id,
                         max(zrt2.zrt_begindatum) zrt_begindatum
                  FROM brk_prep.zakelijk_recht zrt2
                  GROUP BY identificatie, zrt2.ontstaan_uit_asg_id
        ) q
        GROUP BY ontstaan_uit_asg_id
) q2
WHERE q2.ontstaan_uit_asg_id = zrt.betrokken_bij_asg_id;

-- Reference ontstaan_uit and determine max_zrt_begindatum
UPDATE brk_prep.zakelijk_recht zrt
SET ontstaan_uit=q2.ontstaan_uit,
    max_ontstaan_uit_begindatum=q2.max_zrt_begindatum
FROM (
    SELECT
             array_to_json(array_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie)) ontstaan_uit,
             max(zrt_begindatum) max_zrt_begindatum,
             betrokken_bij_asg_id
         FROM (
                  SELECT identificatie,
                         zrt2.betrokken_bij_asg_id,
                         max(zrt2.zrt_begindatum) zrt_begindatum
                  FROM brk_prep.zakelijk_recht zrt2
                  GROUP BY identificatie, zrt2.betrokken_bij_asg_id
        ) q
        GROUP BY betrokken_bij_asg_id
) q2
WHERE q2.betrokken_bij_asg_id = zrt.ontstaan_uit_asg_id;
