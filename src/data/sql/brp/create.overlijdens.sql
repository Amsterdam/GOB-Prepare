SELECT
  ov."BSN"::varchar                                                                AS burgerservicenummer,
  ov."Anummer"::varchar                                                            AS anummer,
  brp_build_date_json(ov."DatumOverlijden")                                        AS datum_overlijden,
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
  brp_build_date_json(ov."DatumIngangOnderzoek")                                   AS datum_ingang_onderzoek,
  brp_build_date_json(ov."DatumEindeOnderzoek")                                    AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,
  brp_build_date_json(ov."DatumGeldigheid")                                        AS ingangsdatum_geldigheid,
  brp_build_date_json(ov."DatumOpname")                                            AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.overlijden ov
