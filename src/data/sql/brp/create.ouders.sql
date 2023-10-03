CREATE TABLE brp_prep.ouders AS

  SELECT
    ouder1."BSN"::varchar                                                                   AS burgerservicenummer,
    ouder1."Anummer"::varchar                                                               AS anummer,

    ouder1."BSNOuder1"::varchar                                                             AS ouder_bSN,
    ouder1."AnummerOuder1"::varchar                                                         AS ouder_anummer,
    ouder1."GeslachtsnaamOuder1"                                                            AS ouder_geslachtsnaam,

    ouder1."VoornamenOuder1"                                                                AS ouder_voornamen,
    ouder1."AdellijketitelOuder1"                                                           AS ouder_adellijke_titel_predikaat,
    ouder1."GeboorteplaatsOmsOuder1"                                                        AS ouder_geboorte_plaats,
    ouder1."GeboortelandOmsOuder1"                                                          AS ouder_geboorte_land,
    CASE -- geboorte datum van de ouder 1
      -- discussion point
      -- complete geboortedatum --> return json {datum, jaar, maand, dag}
      -- What to do if datum = 0
      -- incomplete geboortedatum --> what to do:
      --                                  a) retrun NULL
      --                                  b) retrun default {"datum": "0000-00-00", "jaar": "0000", "maand": "00", "dag": "00"}
      --                                  c) return what is available: {"datum": "1973-00-00", "jaar": "0000", "maand": "00", "dag": "00"}
      WHEN ouder1."GeboortedatumOuder1" IS NULL THEN NULL
      WHEN ouder1."GeboortedatumOuder1" = '0' THEN JSONB_BUILD_OBJECT(
        'datum', '0000-00-00',
        'jaar', '00',
        'maand', '00',
        'dag', '00'
        )
      WHEN length(ouder1."GeboortedatumOuder1") = 8 THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
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
      ELSE NULL
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
    JSONB_BUILD_OBJECT( -- onderzoek ouder 1
      'aanduiding_gegevens_in_onderzoek', ouder1."GegevensInOnderzoekOuder1"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek ouder 1
          WHEN ouder1."GegevensInOnderzoekOuder1" IS NULL THEN NULL
          WHEN ouder1."GegevensInOnderzoekOuder1" = '0' THEN '0000-00-00'
          WHEN length(ouder1."GegevensInOnderzoekOuder1") = 8 THEN CONCAT_WS(
               '_',
              substring(ouder1."GegevensInOnderzoekOuder1", 1, 4),
              substring(ouder1."GegevensInOnderzoekOuder1", 5, 2),
              substring(ouder1."GegevensInOnderzoekOuder1", 7, 2)
            )
          ELSE ouder1."GegevensInOnderzoekOuder1"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek ouder 1
          WHEN ouder1."DatumEindeOnderzoekOuder1" IS NULL THEN NULL
          WHEN ouder1."DatumEindeOnderzoekOuder1" = '0' THEN '0000-00-00'
          WHEN length(ouder1."DatumEindeOnderzoekOuder1") = 8 THEN CONCAT_WS(
              '-',
              substring(ouder1."DatumEindeOnderzoekOuder1", 1, 4),
              substring(ouder1."DatumEindeOnderzoekOuder1", 5, 2),
              substring(ouder1."DatumEindeOnderzoekOuder1", 7, 2)
            )
          ELSE ouder1."DatumEindeOnderzoekOuder1"::varchar
        END,
      'onjuist_strijdig_openbare_orde', NULL::varchar -- unavailable
    )                                                                                          AS onderzoek,
    CASE -- datum opneming ouder 1
      WHEN ouder1."DatumOpnameOuder1" IS NULL THEN NULL
      WHEN ouder1."DatumOpnameOuder1" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
        'datum', '0000-00-00',
        'jaar', '00',
        'maand', '00',
        'dag', '00'
        )
      WHEN length(ouder1."DatumOpnameOuder1") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                                        AS datum_opneming,
    CASE -- datum geldigheid ouder 1
      WHEN ouder1."DatumGeldigheidOuder1" IS NULL THEN NULL
      WHEN ouder1."DatumGeldigheidOuder1" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
        'datum', '0000-00-00',
        'jaar', '00',
        'maand', '00',
        'dag', '00'
        )
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
      ELSE NULL
    END                                                                                        AS ingangsdatum_geldigheid,
    NULL::varchar::date                                                                        AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.personen ouder1

  UNION

  SELECT
    ouder2."BSN"::varchar                                                                   AS burgerservicenummer,
    ouder2."Anummer"::varchar                                                               AS anummer,

    ouder2."BSNOuder2"::varchar                                                             AS ouder_bSN,
    ouder2."AnummerOuder2"::varchar                                                         AS ouder_anummer,
    ouder2."GeslachtsnaamOuder2"                                                            AS ouder_geslachtsnaam,

    ouder2."VoornamenOuder2"                                                                AS ouder_voornamen,
    ouder2."AdellijketitelOuder2"                                                           AS ouder_adellijke_titel_predikaat,
    ouder2."GeboorteplaatsOmsOuder2"                                                        AS ouder_geboorte_plaats,
    ouder2."GeboortelandOmsOuder2"                                                          AS ouder_geboorte_land,
    CASE -- geboorte datum van de ouder 2
      -- discussion point
      -- complete geboortedatum --> return json {datum, jaar, maand, dag}
      -- What to do if datum = 0
      -- incomplete geboortedatum --> what to do:
      --                                  a) retrun NULL
      --                                  b) retrun default {"datum": "0000-00-00", "jaar": "0000", "maand": "00", "dag": "00"}
      --                                  c) return what is available: {"datum": "1973-00-00", "jaar": "0000", "maand": "00", "dag": "00"}
      WHEN ouder2."GeboortedatumOuder2" IS NULL THEN NULL
      WHEN ouder2."GeboortedatumOuder2" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
        'datum', '0000-00-00',
        'jaar', '00',
        'maand', '00',
        'dag', '00'
        )
      WHEN length(ouder2."GeboortedatumOuder2") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
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
    JSONB_BUILD_OBJECT( -- onderzoek ouder 1
      'aanduiding_gegevens_in_onderzoek', ouder2."GegevensInOnderzoekOuder2"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek ouder 1
          WHEN ouder2."GegevensInOnderzoekOuder2" IS NULL THEN NULL
          WHEN ouder2."GegevensInOnderzoekOuder2" = '0' THEN '0000-00-00'
          WHEN length(ouder2."GegevensInOnderzoekOuder2") = 8 THEN CONCAT_WS(
               '_',
              substring(ouder2."GegevensInOnderzoekOuder2", 1, 4),
              substring(ouder2."GegevensInOnderzoekOuder2", 5, 2),
              substring(ouder2."GegevensInOnderzoekOuder2", 7, 2)
            )
          ELSE ouder2."GegevensInOnderzoekOuder2"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek ouder 1
          WHEN ouder2."DatumEindeOnderzoekOuder2" IS NULL THEN NULL
          WHEN ouder2."DatumEindeOnderzoekOuder2" = '0' THEN '0000-00-00'
          WHEN length(ouder2."DatumEindeOnderzoekOuder2") = 8 THEN CONCAT_WS(
              '-',
              substring(ouder2."DatumEindeOnderzoekOuder2", 1, 4),
              substring(ouder2."DatumEindeOnderzoekOuder2", 5, 2),
              substring(ouder2."DatumEindeOnderzoekOuder2", 7, 2)
            )
          ELSE ouder2."DatumEindeOnderzoekOuder2"::varchar
        END,
      'onjuist_strijdig_openbare_orde', NULL::varchar -- unavailable
    )                                                                                          AS onderzoek,
    CASE -- datum opneming ouder 1
      WHEN ouder2."DatumOpnameOuder2" IS NULL THEN NULL
      WHEN ouder2."DatumOpnameOuder2" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
        'datum', '0000-00-00',
        'jaar', '00',
        'maand', '00',
        'dag', '00'
        )
      WHEN length(ouder2."DatumOpnameOuder2") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                                        AS datum_opneming,
    CASE -- datum geldigheid ouder 1
      WHEN ouder2."DatumGeldigheidOuder2" IS NULL THEN NULL
      WHEN ouder2."DatumGeldigheidOuder2" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
        'datum', '0000-00-00',
        'jaar', '00',
        'maand', '00',
        'dag', '00'
        )
      WHEN length(ouder2."DatumGeldigheidOuder2") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                                        AS ingangsdatum_geldigheid,
    NULL::varchar::date                                                                        AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.personen ouder2
