CREATE TABLE brp_prep.kinderen AS

  SELECT
    kindg."BSNOuder"::varchar                                                        AS burgerservicenummer,
    kindg."AnummerOuder"::varchar                                                    AS anummer,

    kindg."BSN"::varchar                                                             AS kind_bSN,
    kindg."Anummer"::varchar                                                         AS kind_anummer,
    kindg."Geslachtsnaam"                                                            AS kind_geslachtsnaam,
    JSONB_BUILD_OBJECT( -- voorvoegsel geslachtsnaam van het kind
      'code', NULL::varchar,
      'omschrijving', kindg."Voorvoegsel"::varchar
    )                                                                                AS kind_voorvoegsel_geslachtsnaam,

    kindg."Voornamen"                                                                AS kind_oornamen,
    kindg."Adellijketitel"                                                           AS kind_adellijke_titel_predicaat,
    kindg."GeboorteplaatsOms"                                                        AS kind_geboorte_plaats,
    kindg."GeboorteplaatsOms"                                                        AS kind_geboorte_land,
    CASE -- geboorte datum van het kind
      -- TODO: Karin wil check if geboortedatum is always an 8 digits!
      -- discussion point
      -- complete geboortedatum --> return datum
      -- incomplete geboortedatum --> what to doe:
      --                                  a) retrun NULL
      --                                  b) retrun default '0000-00-00'
      --                                  c) check the value and return only jaar, or maand or dag
      WHEN kindg."Geboortedatum" IS null THEN NULL
      WHEN kindg."Geboortedatum" = '0'
        OR kindg."Geboortedatum" = '00000000' THEN JSONB_BUILD_OBJECT('datum', '0000-00-00')
      WHEN length(kindg."Geboortedatum") = 8
        AND kindg."Geboortedatum" != '00000000' THEN JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(kindg."Geboortedatum", 1, 4),
          substring(kindg."Geboortedatum", 5, 2),
          substring(kindg."Geboortedatum", 7, 2)
      ))
      ELSE JSONB_BUILD_OBJECT(
        'jaar', substring(kindg."Geboortedatum", 1, 4), -- this may result in error in case that GeboorteDatun not an 8 digits
        'maand', substring(kindg."Geboortedatum", 5, 2),
        'dag', substring(kindg."Geboortedatum", 7, 2)
      )
    END                                                                              AS kind_geboortedatum,

    NULL::varchar                                                                    AS registergemeente_akte, -- unavailable
    NULL::varchar                                                                    AS aktenummer, -- unvailable
    kindg."OntlGemeenteKindgegevens"                                                 AS gemeente_document,

    CASE -- datum document
      WHEN kindg."OntlDatumKindgegevens" IS NULL THEN NULL
      WHEN kindg."OntlDatumKindgegevens" = '0'
        OR kindg."OntlDatumKindgegevens" = '00000000' THEN '0000-00-00'
      WHEN length(kindg."OntlDatumKindgegevens") = 8
        AND kindg."OntlDatumKindgegevens" != '00000000' THEN CONCAT_WS(
          '-',
          substring(kindg."OntlDatumKindgegevens", 1, 4),
          substring(kindg."OntlDatumKindgegevens", 5, 2),
          substring(kindg."OntlDatumKindgegevens", 7, 2)
        )
      ELSE kindg."OntlDatumKindgegevens"
    END                                                                              AS datum_document,

    kindg."Beschrijving document"                                                    AS beschrijvingDocument,

    JSONB_BUILD_OBJECT( -- onderzoek 
      'aanduiding_gegevens_in_onderzoek', kindg."GegevensInOnderzoek"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek
          WHEN kindg."DatumIngangOnderzoek" IS NULL THEN NULL
          WHEN kindg."DatumIngangOnderzoek" = '0'
            OR kindg."DatumIngangOnderzoek" = '00000000' THEN '0000-00-00'
          WHEN length(kindg."DatumIngangOnderzoek") = 8
            AND kindg."DatumIngangOnderzoek" != '00000000' THEN CONCAT(
               '_',
              substring(kindg."DatumIngangOnderzoek", 1, 4),
              substring(kindg."DatumIngangOnderzoek", 5, 2),
              substring(kindg."DatumIngangOnderzoek", 7, 2)
            )
          ELSE kindg."DatumIngangOnderzoek"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek
          WHEN kindg."DatumEindeOnderzoek" IS NULL THEN NULL
          WHEN kindg."DatumEindeOnderzoek" = '0'
            OR kindg."DatumEindeOnderzoek" = '00000000' THEN '0000-00-00'
          WHEN length(kindg."DatumEindeOnderzoek") = 8
            AND kindg."DatumEindeOnderzoek" != '00000000' THEN CONCAT_WS(
              '-',
              substring(kindg."DatumEindeOnderzoek", 1, 4),
              substring(kindg."DatumEindeOnderzoek", 5, 2),
              substring(kindg."DatumEindeOnderzoek", 7, 2)
            )
          ELSE kindg."DatumEindeOnderzoek"::varchar
        END,
      'onjuist_strijdig_openbare_orde', NULL::varchar -- Not available
    )                                                                                AS onderzoek,

    CASE -- datum geldigheid
      WHEN kindg."DatumGeldigheid" IS NULL THEN NULL
      WHEN kindg."DatumGeldigheid" = '0'
        OR kindg."DatumGeldigheid" = '00000000' THEN '0000-00-00'
      WHEN length(kindg."DatumGeldigheid") = 8
        AND kindg."DatumGeldigheid" != '00000000' THEN CONCAT_WS(
          '-',
          substring(kindg."DatumGeldigheid", 1, 4),
          substring(kindg."DatumGeldigheid", 5, 2),
          substring(kindg."DatumGeldigheid", 7, 2)
        )
      ELSE kindg."DatumGeldigheid"
    END                                                                              AS ingangsdatum_geldigheid,

    CASE -- datum opneming
      WHEN kindg."DatumOpname" IS NULL THEN NULL
      WHEN kindg."DatumOpname" = '0'
        OR kindg."DatumOpname" = '00000000' THEN '0000-00-00'
      WHEN length(kindg."DatumOpname") = 8
        AND kindg."DatumOpname" != '00000000' THEN CONCAT_WS(
          '-',
          substring(kindg."DatumOpname", 1, 4),
          substring(kindg."DatumOpname", 5, 2),
          substring(kindg."DatumOpname", 7, 2)
        )
      ELSE kindg."DatumOpname"
    END                                                                              AS datum_opneming,
    NULL::varchar                                                                    AS registratie_betrekking, -- unavailable
    NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.kindgegevens kindg
