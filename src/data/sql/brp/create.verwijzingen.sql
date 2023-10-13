SELECT
  vwg."BSN"::varchar                                                                 AS burgerservicenummer,
  vwg."Anummer"::varchar                                                             AS anummer,
  vwg."VertrekGemeenteCode"::varchar                                                 AS gemeente_uitgeschreven,
  CASE -- datum vertrek
    WHEN vwg."VertrekDatum" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(vwg."VertrekDatum", 1, 4),
          substring(vwg."VertrekDatum", 5, 2),
          substring(vwg."VertrekDatum", 7, 2)
        ),
      'jaar', substring(vwg."VertrekDatum", 1, 4),
      'maand', substring(vwg."VertrekDatum", 5, 2),
      'dag', substring(vwg."VertrekDatum", 7, 2)
      )
  END                                                                                AS datum_uitschrijving,
  JSONB_BUILD_OBJECT(
      'code', vwg."IndGeheimCode",
        'omschrijving', vwg."IndGeheimOms"
  )                                                                                  AS indicatie_geheim,
  NULL::varchar                                                                      AS aanduiding_gegevens_in_onderzoek,
  NULL::date                                                                         AS datum_ingang_onderzoek,
  NULL::date                                                                         AS datum_einde_onderzoek,
  NULL::varchar                                                                      AS onjuist_strijdig_openbare_orde,

  CASE -- datum geldigheid
    WHEN vwg."DatumGeldigheid" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(vwg."DatumGeldigheid", 1, 4),
          substring(vwg."DatumGeldigheid", 5, 2),
          substring(vwg."DatumGeldigheid", 7, 2)
        ),
      'jaar', substring(vwg."DatumGeldigheid", 1, 4),
      'maand', substring(vwg."DatumGeldigheid", 5, 2),
      'dag', substring(vwg."DatumGeldigheid", 7, 2)
      )
  END                                                                                AS ingangsdatum_geldigheid,

  CASE -- datum opneming
    WHEN vwg."DatumOpname" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(vwg."DatumOpname", 1, 4),
          substring(vwg."DatumOpname", 5, 2),
          substring(vwg."DatumOpname", 7, 2)
        ),
      'jaar', substring(vwg."DatumOpname", 1, 4),
      'maand', substring(vwg."DatumOpname", 5, 2),
      'dag', substring(vwg."DatumOpname", 7, 2)
      )
  END                                                                                AS datum_opneming,
  NULL::varchar::date                                                                AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.verwijsgegevens vwg
