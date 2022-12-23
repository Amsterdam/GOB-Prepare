-- Reference betrokken_bij_brk_zakelijke_rechten and determine __max_betrokken_bij_begindatum
UPDATE brk2_prep.zakelijk_recht zrt
SET betrokken_bij_brk_zakelijke_rechten=q2.betrokken_bij_brk_zakelijke_rechten,
    __max_betrokken_bij_begindatum=q2.__max_betrokken_bij_begindatum
FROM (
    SELECT
             json_agg(json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie) betrokken_bij_brk_zakelijke_rechten,
             max(zrt_begindatum) __max_betrokken_bij_begindatum,
             __ontstaan_uit_asg_id
         FROM (
                  SELECT identificatie,
                         zrt2.__ontstaan_uit_asg_id,
                         max(zrt2.begin_geldigheid) zrt_begindatum
                  FROM brk2_prep.zakelijk_recht zrt2
                  GROUP BY identificatie, zrt2.__ontstaan_uit_asg_id
        ) q
        GROUP BY __ontstaan_uit_asg_id
) q2
WHERE q2.__ontstaan_uit_asg_id = zrt.__betrokken_bij_asg_id;

-- Reference ontstaan_uit_brk_zakelijke_rechten and determine __max_ontstaan_uit_begindatum
UPDATE brk2_prep.zakelijk_recht zrt
SET ontstaan_uit_brk_zakelijke_rechten=q2.ontstaan_uit_brk_zakelijke_rechten,
    __max_ontstaan_uit_begindatum=q2.__max_ontstaan_uit_begindatum
FROM (
    SELECT
             json_agg(
                  json_build_object('zrt_identificatie', identificatie) ORDER BY identificatie) ontstaan_uit_brk_zakelijke_rechten,
             max(zrt_begindatum) __max_ontstaan_uit_begindatum,
             __betrokken_bij_asg_id
         FROM (
                  SELECT identificatie,
                         zrt2.__betrokken_bij_asg_id,
                         max(zrt2.begin_geldigheid) zrt_begindatum
                  FROM brk2_prep.zakelijk_recht zrt2
                  GROUP BY identificatie, zrt2.__betrokken_bij_asg_id
        ) q
        GROUP BY __betrokken_bij_asg_id
) q2
WHERE q2.__betrokken_bij_asg_id = zrt.__ontstaan_uit_asg_id;
