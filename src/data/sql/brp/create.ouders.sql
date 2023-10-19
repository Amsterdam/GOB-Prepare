SELECT
  ouder1."BSN"::varchar                                                                      AS burgerservicenummer,
  ouder1."Anummer"::varchar                                                                  AS anummer,
  ouder1."BSNOuder1"::varchar                                                                AS ouder_bSN,
  ouder1."AnummerOuder1"::varchar                                                            AS ouder_anummer,
  ouder1."GeslachtsnaamOuder1"                                                               AS ouder_geslachtsnaam,
  ouder1."VoornamenOuder1"                                                                   AS ouder_voornamen,
  ouder1."AdellijketitelOuder1"                                                              AS ouder_adellijke_titel_predikaat,
  ouder1."GeboorteplaatsOmsOuder1"                                                           AS ouder_geboorte_plaats,
  ouder1."GeboortelandOmsOuder1"                                                             AS ouder_geboorte_land,
  brp_build_date_json(ouder1."GeboortedatumOuder1")                                          AS ouder_geboortedatum,
  ouder1."GeslachtsaanduidingOmsOuder1"                                                      AS ouder_geslachtsaanduiding,
  NULL::varchar                                                                              AS datum_familierechtelijke_betrekking,
  JSONB_BUILD_OBJECT( -- registergemeente akte
    'code', NULL::varchar
  )                                                                                          AS registergemeente_akte,
  JSONB_BUILD_OBJECT( -- akte nummer
    'code', NULL::varchar,
    'omschrijving', NULL::varchar
  )                                                                                          AS aktenummer,
  NULL::varchar                                                                              AS gemeente_document,
  NULL::varchar                                                                              AS datum_document,
  NULL::varchar                                                                              AS beschrijving_document,
  ouder1."GegevensInOnderzoekOuder1"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  brp_build_date_json(ouder1."DatumIngangOnderzoekOuder1")                                   AS datum_ingang_onderzoek,
  brp_build_date_json(ouder1."DatumEindeOnderzoekOuder1")                                    AS datum_einde_onderzoek,
  NULL::varchar                                                                              AS onjuist_strijdig_openbare_orde,
  brp_build_date_json(ouder1."DatumOpnameOuder1")                                            AS datum_opneming,
  brp_build_date_json(ouder1."DatumGeldigheidOuder1")                                        AS ingangsdatum_geldigheid,
  NULL::varchar::date                                                                        AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.personen ouder1

UNION

SELECT
  ouder2."BSN"::varchar                                                                      AS burgerservicenummer,
  ouder2."Anummer"::varchar                                                                  AS anummer,
  ouder2."BSNOuder2"::varchar                                                                AS ouder_bSN,
  ouder2."AnummerOuder2"::varchar                                                            AS ouder_anummer,
  ouder2."GeslachtsnaamOuder2"                                                               AS ouder_geslachtsnaam,
  ouder2."VoornamenOuder2"                                                                   AS ouder_voornamen,
  ouder2."AdellijketitelOuder2"                                                              AS ouder_adellijke_titel_predikaat,
  ouder2."GeboorteplaatsOmsOuder2"                                                           AS ouder_geboorte_plaats,
  ouder2."GeboortelandOmsOuder2"                                                             AS ouder_geboorte_land,
  brp_build_date_json(ouder2."GeboortedatumOuder2")                                          AS ouder_geboortedatum,
  ouder2."GeslachtsaanduidingOmsOuder2"                                                      AS ouder_geslachtsaanduiding,
  NULL::varchar                                                                              AS datum_familierechtelijke_betrekking,
      JSONB_BUILD_OBJECT( -- registergemeente akte
    'code', NULL::varchar
  )                                                                                          AS registergemeente_akte,
  JSONB_BUILD_OBJECT( -- vakte nummer
    'code', NULL::varchar,
    'omschrijving', NULL::varchar
  )                                                                                          AS aktenummer,
  NULL::varchar                                                                              AS gemeente_document,
  NULL::varchar                                                                              AS datum_document,
  NULL::varchar                                                                              AS beschrijving_document,
  ouder2."GegevensInOnderzoekOuder2"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  brp_build_date_json(ouder2."DatumIngangOnderzoekOuder2")                                   AS datum_ingang_onderzoek,
  brp_build_date_json(ouder2."DatumEindeOnderzoekOuder2")                                    AS datum_einde_onderzoek,
  NULL::varchar                                                                              AS onjuist_strijdig_openbare_orde,
  brp_build_date_json(ouder2."DatumOpnameOuder2")                                            AS datum_opneming,
  brp_build_date_json(ouder2."DatumGeldigheidOuder2")                                        AS ingangsdatum_geldigheid,
  NULL::varchar::date                                                                        AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.personen ouder2
