CREATE TABLE brp_prep.ouders AS

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

    CASE -- geboorte datum van de ouder 1
      WHEN ouder1."GeboortedatumOuder1" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(ouder1."GeboortedatumOuder1", 1, 4),
            substring(ouder1."GeboortedatumOuder1", 5, 2),
            substring(ouder1."GeboortedatumOuder1", 7, 2)
          ),
        'jaar', substring(ouder1."GeboortedatumOuder1", 1, 4),
        'maand', substring(ouder1."GeboortedatumOuder1", 5, 2),
        'dag', substring(ouder1."GeboortedatumOuder1", 7, 2)
        )
    END                                                                                        AS ouder_geboortedatum,

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
    CASE -- datum ingang onderzoek
      WHEN ouder1."DatumIngangOnderzoekOuder1" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(ouder1."DatumIngangOnderzoekOuder1", 1, 4),
          substring(ouder1."DatumIngangOnderzoekOuder1", 5, 2),
          substring(ouder1."DatumIngangOnderzoekOuder1", 7, 2)
        ),
        'jaar', substring(ouder1."DatumIngangOnderzoekOuder1", 1, 4),
        'maand', substring(ouder1."DatumIngangOnderzoekOuder1", 5, 2),
        'dag', substring(ouder1."DatumIngangOnderzoekOuder1", 7, 2)
      )
    END                                                                                        AS datum_ingang_onderzoek,

    CASE -- datum einde onderzoek
      WHEN ouder1."DatumEindeOnderzoekOuder1" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(ouder1."DatumEindeOnderzoekOuder1", 1, 4),
          substring(ouder1."DatumEindeOnderzoekOuder1", 5, 2),
          substring(ouder1."DatumEindeOnderzoekOuder1", 7, 2)
        ),
        'jaar', substring(ouder1."DatumEindeOnderzoekOuder1", 1, 4),
        'maand', substring(ouder1."DatumEindeOnderzoekOuder1", 5, 2),
        'dag', substring(ouder1."DatumEindeOnderzoekOuder1", 7, 2)
      )
    END                                                                                        AS datum_einde_onderzoek,
    NULL::varchar                                                                              AS onjuist_strijdig_openbare_orde,

    CASE -- datum opneming ouder 1
      WHEN ouder1."DatumOpnameOuder1" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(ouder1."DatumOpnameOuder1", 1, 4),
            substring(ouder1."DatumOpnameOuder1", 5, 2),
            substring(ouder1."DatumOpnameOuder1", 7, 2)
          ),
        'jaar', substring(ouder1."DatumOpnameOuder1", 1, 4),
        'maand', substring(ouder1."DatumOpnameOuder1", 5, 2),
        'dag', substring(ouder1."DatumOpnameOuder1", 7, 2)
        )
    END                                                                                        AS datum_opneming,
    CASE -- datum geldigheid ouder 1
      WHEN ouder1."DatumGeldigheidOuder1" IS NULL THEN NULL
      WHEN length(ouder1."DatumGeldigheidOuder1") = 8 THEN JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(ouder1."DatumGeldigheidOuder1", 1, 4),
            substring(ouder1."DatumGeldigheidOuder1", 5, 2),
            substring(ouder1."DatumGeldigheidOuder1", 7, 2)
          ),
        'jaar', substring(ouder1."DatumGeldigheidOuder1", 1, 4),
        'maand', substring(ouder1."DatumGeldigheidOuder1", 5, 2),
        'dag', substring(ouder1."DatumGeldigheidOuder1", 7, 2)
        )
    END                                                                                        AS ingangsdatum_geldigheid,
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

    CASE -- geboorte datum van de ouder 2
      WHEN ouder2."GeboortedatumOuder2" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(ouder2."GeboortedatumOuder2", 1, 4),
            substring(ouder2."GeboortedatumOuder2", 5, 2),
            substring(ouder2."GeboortedatumOuder2", 7, 2)
          ),
        'jaar', substring(ouder2."GeboortedatumOuder2", 1, 4),
        'maand', substring(ouder2."GeboortedatumOuder2", 5, 2),
        'dag', substring(ouder2."GeboortedatumOuder2", 7, 2)
        )
    END                                                                                        AS ouder_geboortedatum,

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
    CASE -- datum ingang onderzoek
      WHEN ouder2."DatumIngangOnderzoekOuder2" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(ouder2."DatumIngangOnderzoekOuder2", 1, 4),
          substring(ouder2."DatumIngangOnderzoekOuder2", 5, 2),
          substring(ouder2."DatumIngangOnderzoekOuder2", 7, 2)
        ),
        'jaar', substring(ouder2."DatumIngangOnderzoekOuder2", 1, 4),
        'maand', substring(ouder2."DatumIngangOnderzoekOuder2", 5, 2),
        'dag', substring(ouder2."DatumIngangOnderzoekOuder2", 7, 2)
      )
    END                                                                                        AS datum_ingang_onderzoek,

    CASE -- datum einde onderzoek
      WHEN ouder2."DatumEindeOnderzoekOuder2" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(ouder2."DatumEindeOnderzoekOuder2", 1, 4),
          substring(ouder2."DatumEindeOnderzoekOuder2", 5, 2),
          substring(ouder2."DatumEindeOnderzoekOuder2", 7, 2)
        ),
        'jaar', substring(ouder2."DatumEindeOnderzoekOuder2", 1, 4),
        'maand', substring(ouder2."DatumEindeOnderzoekOuder2", 5, 2),
        'dag', substring(ouder2."DatumEindeOnderzoekOuder2", 7, 2)
      )
    END                                                                                        AS datum_einde_onderzoek,
    NULL::varchar                                                                              AS onjuist_strijdig_openbare_orde,

    CASE -- datum opneming ouder 1
      WHEN ouder2."DatumOpnameOuder2" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(ouder2."DatumOpnameOuder2", 1, 4),
            substring(ouder2."DatumOpnameOuder2", 5, 2),
            substring(ouder2."DatumOpnameOuder2", 7, 2)
          ),
        'jaar', substring(ouder2."DatumOpnameOuder2", 1, 4),
        'maand', substring(ouder2."DatumOpnameOuder2", 5, 2),
        'dag', substring(ouder2."DatumOpnameOuder2", 7, 2)
        )
    END                                                                                        AS datum_opneming,
    CASE -- datum geldigheid ouder 1
      WHEN ouder2."DatumGeldigheidOuder2" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(ouder2."DatumGeldigheidOuder2", 1, 4),
            substring(ouder2."DatumGeldigheidOuder2", 5, 2),
            substring(ouder2."DatumGeldigheidOuder2", 7, 2)
          ),
        'jaar', substring(ouder2."DatumGeldigheidOuder2", 1, 4),
        'maand', substring(ouder2."DatumGeldigheidOuder2", 5, 2),
        'dag', substring(ouder2."DatumGeldigheidOuder2", 7, 2)
        )
    END                                                                                        AS ingangsdatum_geldigheid,
    NULL::varchar::date                                                                        AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.personen ouder2
