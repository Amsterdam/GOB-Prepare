SELECT
  ov."BSN"::varchar                                                                AS burgerservicenummer,
  ov."Anummer"::varchar                                                            AS anummer,

  CASE -- datum overlijden
    WHEN ov."DatumOverlijden" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(ov."DatumOverlijden", 1, 4),
          substring(ov."DatumOverlijden", 5, 2),
          substring(ov."DatumOverlijden", 7, 2)
        ),
      'jaar', substring(ov."DatumOverlijden", 1, 4),
      'maand', substring(ov."DatumOverlijden", 5, 2),
      'dag', substring(ov."DatumOverlijden", 7, 2)
      )
  END                                                                              AS datum_overlijden,
  ov."GemeenteOmsOverlijden"::varchar                                              AS plaats_overlijden,
  ov."LandOmsOverlijden"::varchar                                                  AS land_overlijden,

  JSONB_BUILD_OBJECT( -- registergemeente akte
    'code',ov."RegisterGemeente"::varchar
  )                                                                                AS registergemeente_akte,
  JSONB_BUILD_OBJECT( -- vakte nummer
    'code', ov."AkteNr"::varchar,
    'omschrijving', NULL::varchar
  )                                                                                AS aktenummer,
  ov."RegisterGemeente"::varchar                                                   AS gemeente_document,
  NULL::varchar                                                                    AS datum_document,
  NULL::varchar                                                                    AS beschrijving_document,

  ov."GegevensInOnderzoek"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  CASE -- datum ingang onderzoek
    WHEN ov."DatumIngangOnderzoek" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(ov."DatumIngangOnderzoek", 1, 4),
        substring(ov."DatumIngangOnderzoek", 5, 2),
        substring(ov."DatumIngangOnderzoek", 7, 2)
      ),
      'jaar', substring(ov."DatumIngangOnderzoek", 1, 4),
      'maand', substring(ov."DatumIngangOnderzoek", 5, 2),
      'dag', substring(ov."DatumIngangOnderzoek", 7, 2)
    )
  END                                                                              AS datum_ingang_onderzoek,
  CASE -- datum einde onderzoek
    WHEN ov."DatumEindeOnderzoek" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(ov."DatumEindeOnderzoek", 1, 4),
        substring(ov."DatumEindeOnderzoek", 5, 2),
        substring(ov."DatumEindeOnderzoek", 7, 2)
      ),
      'jaar', substring(ov."DatumEindeOnderzoek", 1, 4),
      'maand', substring(ov."DatumEindeOnderzoek", 5, 2),
      'dag', substring(ov."DatumEindeOnderzoek", 7, 2)
    )
  END                                                                              AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  CASE -- datum geldigheid
    WHEN ov."DatumGeldigheid" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(ov."DatumGeldigheid", 1, 4),
          substring(ov."DatumGeldigheid", 5, 2),
          substring(ov."DatumGeldigheid", 7, 2)
        ),
      'jaar', substring(ov."DatumGeldigheid", 1, 4),
      'maand', substring(ov."DatumGeldigheid", 5, 2),
      'dag', substring(ov."DatumGeldigheid", 7, 2)
      )
  END                                                                              AS ingangsdatum_geldigheid,
  CASE -- datum opneming
    WHEN ov."DatumOpname" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(ov."DatumOpname", 1, 4),
          substring(ov."DatumOpname", 5, 2),
          substring(ov."DatumOpname", 7, 2)
        ),
      'jaar', substring(ov."DatumOpname", 1, 4),
      'maand', substring(ov."DatumOpname", 5, 2),
      'dag', substring(ov."DatumOpname", 7, 2)
      )
  END                                                                              AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.overlijden ov
