CREATE TABLE brp_prep.huwelijkenpartnerschappen AS

  SELECT
    hwp."BSN"::varchar                                                                AS burgerservicenummer,
    hwp."Anummer"::varchar                                                            AS anummer,

    hwp."BSNPartner"::varchar                                                         AS partner_bSN,
    hwp."AnummerPartner"::varchar                                                     AS partner_anummer,
    hwp."Geslachtsnaam"                                                               AS partner_geslachtsnaam,

   JSONB_BUILD_OBJECT( -- voorvoegsel geslachtsnaam van de partner
      'code', NULL::varchar,
      'omschrijving', hwp."Voorvoegsel"::varchar
    )                                                                                AS partner_voorvoegsel_geslachtsnaam,

    hwp."Voornamen"                                                                   AS partner_voornamen,

   JSONB_BUILD_OBJECT( -- adellijke titel predikaat van de partner
      'code', hwp."AdellijketitelCode"::varchar,
      'omschrijving', hwp."AdellijketitelOms"::varchar
    )                                                                                AS partner_adellijke_titel_predikaat,

    hwp."GeboorteplaatsOms"                                                           AS partner_geboortePlaats,
    hwp."GeboortelandOms"                                                             AS partner_geboorteLand,
    CASE -- geboorte datum van de partner
      WHEN hwp."Geboortedatum" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(hwp."Geboortedatum", 1, 4),
            substring(hwp."Geboortedatum", 5, 2),
            substring(hwp."Geboortedatum", 7, 2)
          ),
        'jaar', substring(hwp."Geboortedatum", 1, 4),
        'maand', substring(hwp."Geboortedatum", 5, 2),
        'dag', substring(hwp."Geboortedatum", 7, 2)
        )
    END                                                                              AS partner_geboortedatum,

    hwp."GeslachtsaanduidingOms"                                                     AS partner_geslachtsaanduiding,

    CASE -- datum van sluiting huwelijk partnerschap
      WHEN hwp."DatumSluitingHuwelijkPartnerschap" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(hwp."DatumSluitingHuwelijkPartnerschap", 1, 4),
            substring(hwp."DatumSluitingHuwelijkPartnerschap", 5, 2),
            substring(hwp."DatumSluitingHuwelijkPartnerschap", 7, 2)
          ),
        'jaar', substring(hwp."DatumSluitingHuwelijkPartnerschap", 1, 4),
        'maand', substring(hwp."DatumSluitingHuwelijkPartnerschap", 5, 2),
        'dag', substring(hwp."DatumSluitingHuwelijkPartnerschap", 7, 2)
        )
    END                                                                              AS partner_datum_sluiting_huwelijk_partnerschap,

    JSONB_BUILD_OBJECT( -- plaats sluiting huwelijk
      'code', hwp."PlaatsSluitingHuwelijkPartnerschapCode"::varchar,
      'omschrijving', hwp."PlaatsSluitingHuwelijkPartnerschapOms"::varchar
    )                                                                                AS partner_plaats_sluiting_huwelijk_partnerschap,

    JSONB_BUILD_OBJECT( -- land sluiting huwelijk
      'code', hwp."LandSluitingHuwelijkPartnerschapCode"::varchar,
      'omschrijving', hwp."LandSluitingHuwelijkPartnerschapOms"::varchar
    )                                                                                AS partner_land_sluiting_huwelijk_partnerschap,

    CASE -- datum van ontbinding huwelijk
      WHEN hwp."DatumOntbindingHuwelijkPartnerschap" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(hwp."DatumOntbindingHuwelijkPartnerschap", 1, 4),
            substring(hwp."DatumOntbindingHuwelijkPartnerschap", 5, 2),
            substring(hwp."DatumOntbindingHuwelijkPartnerschap", 7, 2)
          ),
        'jaar', substring(hwp."DatumOntbindingHuwelijkPartnerschap", 1, 4),
        'maand', substring(hwp."DatumOntbindingHuwelijkPartnerschap", 5, 2),
        'dag', substring(hwp."DatumOntbindingHuwelijkPartnerschap", 7, 2)
        )
    END                                                                              AS partner_datum_ontbinding_huwelijk_partnerschap,

    JSONB_BUILD_OBJECT( -- plaats ontbinding huwelijk
      'code', hwp."PlaatsOntbindingHuwelijkPartnerschapCode"::varchar,
      'omschrijving', hwp."PlaatsOntbindingHuwelijkPartnerschapOms"::varchar
    )                                                                                AS partner_plaats_ontbinding_huwelijk_partnerschap,

    JSONB_BUILD_OBJECT( -- land ontbinding huwelijk
      'code', hwp."LandOntbindingHuwelijkPartnerschapCode"::varchar,
      'omschrijving', hwp."LandOntbindingHuwelijkPartnerschapOms"::varchar
    )                                                                                AS partner_land_ontbinding_huwelijk_partnerschap,

    JSONB_BUILD_OBJECT( -- reden ontbinding huwelijk
      'code', hwp."RedenOntbindingHuwelijkPartnerschapCode"::varchar,
      'omschrijving', hwp."RedenOntbindingHuwelijkPartnerschapOms"::varchar
    )                                                                                AS partner_reden_ontbinding_huwelijk_partnerschap,

    hwp."SoortVerbintenis"                                                           AS partner_soort_verbintenis,
        JSONB_BUILD_OBJECT( -- geregistergemeente akte
      'code', hwp."AkteGemeente"
    )                                                                                AS registergemeente_akte,
    JSONB_BUILD_OBJECT( -- akte nummer
      'code', hwp."AkteNr",
      'omschrijving', NULL::varchar
    )                                                                                AS aktenummer,
    NULL::varchar                                                                    AS gemeente_document,
    NULL::varchar                                                                    AS datum_document,
    NULL::varchar                                                                    AS beschrijving_document,

    hwp."GegevensInOnderzoek"::varchar                                               AS aanduiding_gegevens_in_onderzoek,
    CASE -- datum ingang onderzoek
      WHEN hwp."DatumIngangOnderzoek" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(hwp."DatumIngangOnderzoek", 1, 4),
          substring(hwp."DatumIngangOnderzoek", 5, 2),
          substring(hwp."DatumIngangOnderzoek", 7, 2)
        ),
        'jaar', substring(hwp."DatumIngangOnderzoek", 1, 4),
        'maand', substring(hwp."DatumIngangOnderzoek", 5, 2),
        'dag', substring(hwp."DatumIngangOnderzoek", 7, 2)
      )
    END                                                                              AS datum_ingang_onderzoek,

    CASE -- datum einde onderzoek
      WHEN hwp."DatumEindeOnderzoek" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(hwp."DatumEindeOnderzoek", 1, 4),
          substring(hwp."DatumEindeOnderzoek", 5, 2),
          substring(hwp."DatumEindeOnderzoek", 7, 2)
        ),
        'jaar', substring(hwp."DatumEindeOnderzoek", 1, 4),
        'maand', substring(hwp."DatumEindeOnderzoek", 5, 2),
        'dag', substring(hwp."DatumEindeOnderzoek", 7, 2)
      )
    END                                                                              AS datum_einde_onderzoek,
    NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

    CASE -- datum geldigheid
      WHEN hwp."DatumGeldigheid" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(hwp."DatumGeldigheid", 1, 4),
            substring(hwp."DatumGeldigheid", 5, 2),
            substring(hwp."DatumGeldigheid", 7, 2)
          ),
        'jaar', substring(hwp."DatumGeldigheid", 1, 4),
        'maand', substring(hwp."DatumGeldigheid", 5, 2),
        'dag', substring(hwp."DatumGeldigheid", 7, 2)
        )
    END                                                                              AS ingangsdatum_geldigheid,

    CASE -- datum opneming
      WHEN hwp."DatumOpname" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(hwp."DatumOpname", 1, 4),
            substring(hwp."DatumOpname", 5, 2),
            substring(hwp."DatumOpname", 7, 2)
          ),
        'jaar', substring(hwp."DatumOpname", 1, 4),
        'maand', substring(hwp."DatumOpname", 5, 2),
        'dag', substring(hwp."DatumOpname", 7, 2)
        )
    END                                                                              AS datum_opneming,
    NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.huwelijk hwp
