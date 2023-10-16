SELECT
  rd."BSN"::varchar                                                                AS burgerservicenummer,
  rd."Anummer"::varchar                                                            AS anummer,
  JSONB_BUILD_OBJECT( -- nationaliteit
    'code', NULL::varchar,
    'omschrijving', rd."SoortNedReisdocument"::varchar
    )                                                                              AS soort_nl_reisdocument,
  rd."NummerNedReisdocument"                                                       AS nummer_nl_reisdocument,
  brp_datum_prefix_rd."DatumUitgifte"                                              AS datum_verstrekking_nl_reisdocument,
  rd."AutoriteitAfgifte"                                                           AS autoriteit_nl_reisdocument,
  brp_datum_prefix_rd."DatumEindeGeldigheid"                                       AS datum_einde_geldigheid_nl_reisdocument,
  brp_datum_prefix_rd."DatumInhoudingVermissing"                                   AS datum_inhouding_nl_reisdocument,
  NULL::varchar                                                                    AS aanduiding_inhouding_nl_reisdocument,
  NULL::varchar                                                                    AS lengte_houder,
  rd."SignaleringNedReisdocument"                                                  AS signalering_nl_reisdocument,
  rd."GemeenteOntleningCode"                                                       AS gemeente_document,
  NULL                                                                             AS datum_document,
  rd."BeschrijvingDocument"                                                        AS beschrijving_document,

  NULL::varchar                                                                    AS aanduiding_gegevens_in_onderzoek,
  brp_datum_prefix_rd."DatumIngangOnderzoek"                                       AS datum_ingang_onderzoek,
  brp_datum_prefix_rd."DatumEindeOnderzoek"                                        AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  brp_datum_prefix_rd."DatumGeldigheid"                                            AS ingangsdatum_geldigheid,
  brp_datum_prefix_rd."DatumOpname"                                                AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- still have to decide what will be

FROM brp.reisdocumenten rd
