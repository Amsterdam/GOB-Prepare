CREATE TABLE brp_prep.kinderen AS

  SELECT
    kind."BSNOuder"::varchar                                                        AS burgerservicenummer,
    kind."AnummerOuder"::varchar                                                    AS anummer,

    kind."BSN"::varchar                                                             AS kind_bSN,
    kind."Anummer"::varchar                                                         AS kind_anummer,
    kind."Geslachtsnaam"                                                            AS kind_geslachtsnaam,
    JSONB_BUILD_OBJECT( -- voorvoegsel geslachtsnaam van het kind
      'code', NULL::varchar,
      'omschrijving', kind."Voorvoegsel"::varchar
    )                                                                                AS kind_voorvoegsel_geslachtsnaam,

    kind."Voornamen"                                                                AS kind_oornamen,
    kind."Adellijketitel"                                                           AS kind_adellijke_titel_predicaat,
    kind."GeboorteplaatsOms"                                                        AS kind_geboorte_plaats,
    kind."GeboorteplaatsOms"                                                        AS kind_geboorte_land,
    CASE -- geboorte datum van het kind
      WHEN kind."Geboortedatum" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kind."Geboortedatum", 1, 4),
            substring(kind."Geboortedatum", 5, 2),
            substring(kind."Geboortedatum", 7, 2)
          ),
        'jaar', substring(kind."Geboortedatum", 1, 4),
        'maand', substring(kind."Geboortedatum", 5, 2),
        'dag', substring(kind."Geboortedatum", 7, 2)
        )
    END                                                                              AS kind_geboortedatum,
    JSONB_BUILD_OBJECT( -- registergemeente akte
      'code', NULL::varchar
    )                                                                                AS registergemeente_akte, -- unavailable
    JSONB_BUILD_OBJECT( -- akte nummer
      'code', NULL::varchar,
      'omschrijving', NULL::varchar
    )                                                                                AS aktenummer, -- unvailable
    kind."OntlGemeenteKindgegevens"                                                 AS gemeente_document,

    CASE -- datum document
      WHEN kind."OntlDatumKindgegevens" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kind."OntlDatumKindgegevens", 1, 4),
            substring(kind."OntlDatumKindgegevens", 5, 2),
            substring(kind."OntlDatumKindgegevens", 7, 2)
          ),
        'jaar', substring(kind."OntlDatumKindgegevens", 1, 4),
        'maand', substring(kind."OntlDatumKindgegevens", 5, 2),
        'dag', substring(kind."OntlDatumKindgegevens", 7, 2)
        )
    END                                                                              AS datum_document,

    kind."Beschrijving document"                                                     AS beschrijving_document,
    kind."GegevensInOnderzoek"::varchar                                              AS aanduiding_gegevens_in_onderzoek,
    CASE -- datum ingang onderzoek
      WHEN kind."DatumIngangOnderzoek" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(kind."DatumIngangOnderzoek", 1, 4),
          substring(kind."DatumIngangOnderzoek", 5, 2),
          substring(kind."DatumIngangOnderzoek", 7, 2)
        ),
        'jaar', substring(kind."DatumIngangOnderzoek", 1, 4),
        'maand', substring(kind."DatumIngangOnderzoek", 5, 2),
        'dag', substring(kind."DatumIngangOnderzoek", 7, 2)
      )
    END                                                                              AS datum_ingang_onderzoek,
    CASE -- datum einde onderzoek
      WHEN kind."DatumEindeOnderzoek" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(kind."DatumEindeOnderzoek", 1, 4),
          substring(kind."DatumEindeOnderzoek", 5, 2),
          substring(kind."DatumEindeOnderzoek", 7, 2)
        ),
        'jaar', substring(kind."DatumEindeOnderzoek", 1, 4),
        'maand', substring(kind."DatumEindeOnderzoek", 5, 2),
        'dag', substring(kind."DatumEindeOnderzoek", 7, 2)
      )
    END                                                                              AS datum_einde_onderzoek,
    NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

    CASE -- datum geldigheid
      WHEN kind."DatumGeldigheid" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kind."DatumGeldigheid", 1, 4),
            substring(kind."DatumGeldigheid", 5, 2),
            substring(kind."DatumGeldigheid", 7, 2)
          ),
        'jaar', substring(kind."DatumGeldigheid", 1, 4),
        'maand', substring(kind."DatumGeldigheid", 5, 2),
        'dag', substring(kind."DatumGeldigheid", 7, 2)
        )
    END                                                                              AS ingangsdatum_geldigheid,

    CASE -- datum opneming
      WHEN kind."DatumOpname" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kind."DatumOpname", 1, 4),
            substring(kind."DatumOpname", 5, 2),
            substring(kind."DatumOpname", 7, 2)
          ),
        'jaar', substring(kind."DatumOpname", 1, 4),
        'maand', substring(kind."DatumOpname", 5, 2),
        'dag', substring(kind."DatumOpname", 7, 2)
        )
    END                                                                              AS datum_opneming,
    NULL::varchar                                                                    AS registratie_betrekking, -- unavailable
    NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.kindgegevens kind
