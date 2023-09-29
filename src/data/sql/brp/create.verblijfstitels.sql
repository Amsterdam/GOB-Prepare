CREATE TABLE brp_prep.verblijfstitels AS

  SELECT
    vt."BSN"::varchar                                                                AS burgerservicenummer,
    vt."Anummer"::varchar                                                            AS anummer,
    JSONB_BUILD_OBJECT( -- nationaliteit
      'code', vt."VerblijftitelCode"::varchar,
      'omschrijving', vt."VerblijftitelOms"::text
    )                                                                                AS aanduiding_verblijfstitel,
    CASE -- datum verkrijging
      WHEN vt."DatumVerkrijging" IS NULL THEN NULL
      WHEN vt."DatumVerkrijging" = '0' THEN '0000-00-00'
      WHEN length(vt."DatumVerkrijging") = 8
        AND vt."DatumVerkrijging" != '00000000' THEN CONCAT(
          substring(vt."DatumVerkrijging", 1, 4),
          '-',
          substring(vt."DatumVerkrijging", 5, 2),
          '-',
          substring(vt."DatumVerkrijging", 7, 2)
        )
      ELSE vt."DatumVerkrijging"
    END                                                                              AS ingangsdatum_verblijfstitel,
    CASE -- datum verlies
      WHEN vt."DatumVerlies" IS NULL THEN NULL
      WHEN vt."DatumVerlies" = '0' THEN '0000-00-00'
      WHEN length(vt."DatumVerlies") = 8
        AND vt."DatumVerlies" != '00000000' THEN CONCAT(
          substring(vt."DatumVerlies", 1, 4),
          '-',
          substring(vt."DatumVerlies", 5, 2),
          '-',
          substring(vt."DatumVerlies", 7, 2)
        )
      ELSE vt."DatumVerlies"
    END                                                                              AS datum_einde_verblijfstitel,
    JSONB_BUILD_OBJECT( -- onderzoek 
      'aanduiding_gegevens_in_onderzoek', vt."GegevensInOnderzoek"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek
          WHEN vt."DatumIngangOnderzoek" IS NULL THEN NULL
          WHEN vt."DatumIngangOnderzoek" = '0' THEN '0000-00-00'
          WHEN length(vt."DatumIngangOnderzoek") = 8
            AND vt."DatumIngangOnderzoek" != '00000000' THEN CONCAT(
            substring(vt."DatumIngangOnderzoek", 1, 4),
              '-',
              substring(vt."DatumIngangOnderzoek", 5, 2),
              '-',
              substring(vt."DatumIngangOnderzoek", 7, 2)
            )
          ELSE vt."DatumIngangOnderzoek"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek
          WHEN vt."DatumEindeOnderzoek" IS NULL THEN NULL
          WHEN vt."DatumEindeOnderzoek" = '0' THEN '0000-00-00'
          WHEN length(vt."DatumEindeOnderzoek") = 8
            AND vt."DatumEindeOnderzoek" != '00000000' THEN CONCAT(
              substring(vt."DatumEindeOnderzoek", 1, 4),
              '-',
              substring(vt."DatumEindeOnderzoek", 5, 2),
              '-',
              substring(vt."DatumEindeOnderzoek", 7, 2)
            )
          ELSE vt."DatumEindeOnderzoek"::varchar
        END,
        'onjuist_strijdig_openbare_orde', NULL::text
    )                                                                                AS onderzoek,
    CASE -- datum geldigheid
      WHEN vt."DatumGeldigheid" IS NULL THEN NULL
      WHEN vt."DatumGeldigheid" = '0' THEN '0000-00-00'
      WHEN length(vt."DatumGeldigheid") = 8
        AND vt."DatumGeldigheid" != '00000000' THEN CONCAT(
          substring(vt."DatumGeldigheid", 1, 4),
          '-',
          substring(vt."DatumGeldigheid", 5, 2),
          '-',
          substring(vt."DatumGeldigheid", 7, 2)
        )
      ELSE vt."DatumGeldigheid"
    END                                                                              AS ingangsdatum_geldigheid,
    CASE -- datum opneming
      WHEN vt."DatumOpname" IS NULL THEN NULL
      WHEN vt."DatumOpname" = '0' THEN '0000-00-00'
      WHEN length(vt."DatumOpname") = 8
        AND vt."DatumOpname" != '00000000' THEN CONCAT(
          substring(vt."DatumOpname", 1, 4),
          '-',
          substring(vt."DatumOpname", 5, 2),
          '-',
          substring(vt."DatumOpname", 7, 2)
        )
      ELSE vt."DatumOpname"
    END                                                                              AS datum_opneming,
    NULL::text::date                                                                 AS datum_actueel_tot -- still have to decide what will be

  FROM brp.verblijfstitel vt
