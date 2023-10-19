SELECT
  hwp."BSN"::varchar                                                                 AS burgerservicenummer,
  hwp."Anummer"::varchar                                                             AS anummer,
  hwp."BSNPartner"::varchar                                                          AS partner_bSN,
  hwp."AnummerPartner"::varchar                                                      AS partner_anummer,
  hwp."Geslachtsnaam"                                                                AS partner_geslachtsnaam,
  JSONB_BUILD_OBJECT( -- voorvoegsel geslachtsnaam van de partner
    'code', NULL::varchar,
    'omschrijving', hwp."Voorvoegsel"::varchar
  )                                                                                  AS partner_voorvoegsel_geslachtsnaam,
  hwp."Voornamen"                                                                    AS partner_voornamen,
  JSONB_BUILD_OBJECT( -- adellijke titel predikaat van de partner
    'code', hwp."AdellijketitelCode"::varchar,
    'omschrijving', hwp."AdellijketitelOms"::varchar
  )                                                                                  AS partner_adellijke_titel_predikaat,
  hwp."GeboorteplaatsOms"                                                            AS partner_geboortePlaats,
  hwp."GeboortelandOms"                                                              AS partner_geboorteLand,
  brp_build_date_json(hwp."Geboortedatum")                                           AS partner_geboortedatum,
  hwp."GeslachtsaanduidingOms"                                                       AS partner_geslachtsaanduiding,
  brp_build_date_json(hwp."DatumSluitingHuwelijkPartnerschap")                       AS partner_datum_sluiting_huwelijk_partnerschap,
  JSONB_BUILD_OBJECT( -- plaats sluiting huwelijk
    'code', hwp."PlaatsSluitingHuwelijkPartnerschapCode"::varchar,
    'omschrijving', hwp."PlaatsSluitingHuwelijkPartnerschapOms"::varchar
  )                                                                                  AS partner_plaats_sluiting_huwelijk_partnerschap,
  JSONB_BUILD_OBJECT( -- land sluiting huwelijk
    'code', hwp."LandSluitingHuwelijkPartnerschapCode"::varchar,
    'omschrijving', hwp."LandSluitingHuwelijkPartnerschapOms"::varchar
  )                                                                                  AS partner_land_sluiting_huwelijk_partnerschap,
  brp_build_date_json(hwp."DatumOntbindingHuwelijkPartnerschap")                     AS partner_datum_ontbinding_huwelijk_partnerschap,
  JSONB_BUILD_OBJECT( -- plaats ontbinding huwelijk
    'code', hwp."PlaatsOntbindingHuwelijkPartnerschapCode"::varchar,
    'omschrijving', hwp."PlaatsOntbindingHuwelijkPartnerschapOms"::varchar
  )                                                                                  AS partner_plaats_ontbinding_huwelijk_partnerschap,
  JSONB_BUILD_OBJECT( -- land ontbinding huwelijk
    'code', hwp."LandOntbindingHuwelijkPartnerschapCode"::varchar,
    'omschrijving', hwp."LandOntbindingHuwelijkPartnerschapOms"::varchar
  )                                                                                  AS partner_land_ontbinding_huwelijk_partnerschap,
  JSONB_BUILD_OBJECT( -- reden ontbinding huwelijk
    'code', hwp."RedenOntbindingHuwelijkPartnerschapCode"::varchar,
    'omschrijving', hwp."RedenOntbindingHuwelijkPartnerschapOms"::varchar
  )                                                                                  AS partner_reden_ontbinding_huwelijk_partnerschap,
  hwp."SoortVerbintenis"                                                             AS partner_soort_verbintenis,
      JSONB_BUILD_OBJECT( -- geregistergemeente akte
    'code', hwp."RegisterGemeente"
  )                                                                                  AS registergemeente_akte,
  JSONB_BUILD_OBJECT( -- akte nummer
    'code', hwp."AkteNr",
    'omschrijving', NULL::varchar
  )                                                                                  AS aktenummer,
  NULL::varchar                                                                      AS gemeente_document,
  NULL::varchar                                                                      AS datum_document,
  NULL::varchar                                                                      AS beschrijving_document,

  hwp."GegevensInOnderzoek"::varchar                                                 AS aanduiding_gegevens_in_onderzoek,
  brp_build_date_json(hwp."DatumIngangOnderzoek")                                    AS datum_ingang_onderzoek,
  brp_build_date_json(hwp."DatumEindeOnderzoek")                                     AS datum_einde_onderzoek,
  NULL::varchar                                                                      AS onjuist_strijdig_openbare_orde,
  brp_build_date_json(hwp."DatumGeldigheid")                                         AS ingangsdatum_geldigheid,
  brp_build_date_json(hwp."DatumOpname")                                             AS datum_opneming,
  NULL::varchar::date                                                                AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.huwelijk hwp
