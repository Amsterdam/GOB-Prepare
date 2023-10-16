SELECT
  vwg."BSN"::varchar                                                                 AS burgerservicenummer,
  vwg."Anummer"::varchar                                                             AS anummer,
  vwg."VertrekGemeenteCode"::varchar                                                 AS gemeente_uitgeschreven,

  brp_datum_prefix_vwg."VertrekDatum"                                                 AS datum_uitschrijving,
  JSONB_BUILD_OBJECT(
      'code', vwg."IndGeheimCode",
        'omschrijving', vwg."IndGeheimOms"
  )                                                                                  AS indicatie_geheim,
  NULL::varchar                                                                      AS aanduiding_gegevens_in_onderzoek,
  NULL::date                                                                         AS datum_ingang_onderzoek,
  NULL::date                                                                         AS datum_einde_onderzoek,
  NULL::varchar                                                                      AS onjuist_strijdig_openbare_orde,

  brp_datum_prefix_vwg."DatumGeldigheid"                                             AS ingangsdatum_geldigheid,

  brp_datum_prefix_vwg."DatumOpname"                                                 AS datum_opneming,
  NULL::varchar::date                                                                AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.verwijsgegevens vwg
