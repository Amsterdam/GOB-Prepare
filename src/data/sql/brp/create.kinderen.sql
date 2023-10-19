SELECT
  kind."BSNOuder"::varchar                                                         AS burgerservicenummer,
  kind."AnummerOuder"::varchar                                                     AS anummer,
  kind."BSN"::varchar                                                              AS kind_bSN,
  kind."Anummer"::varchar                                                          AS kind_anummer,
  kind."Geslachtsnaam"                                                             AS kind_geslachtsnaam,
  JSONB_BUILD_OBJECT( -- voorvoegsel geslachtsnaam van het kind
    'code', NULL::varchar,
    'omschrijving', kind."Voorvoegsel"::varchar
  )                                                                                AS kind_voorvoegsel_geslachtsnaam,
  kind."Voornamen"                                                                 AS kind_oornamen,
  kind."Adellijketitel"                                                            AS kind_adellijke_titel_predicaat,
  kind."GeboorteplaatsOms"                                                         AS kind_geboorte_plaats,
  kind."GeboorteplaatsOms"                                                         AS kind_geboorte_land,
  brp_build_date_json(kind."Geboortedatum")                                        AS kind_geboortedatum,
  JSONB_BUILD_OBJECT( -- registergemeente akte
    'code', NULL::varchar
  )                                                                                AS registergemeente_akte, -- unavailable
  JSONB_BUILD_OBJECT( -- akte nummer
    'code', NULL::varchar,
    'omschrijving', NULL::varchar
  )                                                                                AS aktenummer, -- unvailable
  kind."OntlGemeenteKindgegevens"                                                  AS gemeente_document,
  brp_build_date_json(kind."OntlDatumKindgegevens")                                AS datum_document,
  kind."Beschrijving document"                                                     AS beschrijving_document,
  kind."GegevensInOnderzoek"::varchar                                              AS aanduiding_gegevens_in_onderzoek,
  brp_build_date_json(kind."DatumIngangOnderzoek")                                 AS datum_ingang_onderzoek,
  brp_build_date_json(kind."DatumEindeOnderzoek")                                  AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,
  NULL::varchar                                                                    AS registratie_betrekking, -- unavailable
  brp_build_date_json(kind."DatumGeldigheid")                                      AS ingangsdatum_geldigheid,
  brp_build_date_json(kind."DatumOpname")                                          AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.kindgegevens kind
