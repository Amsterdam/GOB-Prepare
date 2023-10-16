SELECT
  prs."BSN"::varchar                                                               AS burgerservicenummer,
  prs."Anummer"::varchar                                                           AS anummer,
  CASE -- adresseert verblijfsobject
    WHEN prs."IdentificatiecodeVerblijfplaats" IS NULL THEN NULL
    WHEN substring(prs."IdentificatiecodeVerblijfplaats", 5, 2) = '01'
      THEN prs."IdentificatiecodeVerblijfplaats"
    ELSE NULL
  END                                                                              AS adresseert_bag_verblijfsobject,
  CASE -- adresseert ligplaats
    WHEN prs."IdentificatiecodeVerblijfplaats" IS NULL THEN NULL
    WHEN substring(prs."IdentificatiecodeVerblijfplaats", 5, 2) = '02'
      THEN prs."IdentificatiecodeVerblijfplaats"
    ELSE NULL
  END                                                                              AS adresseert_bag_ligplaats,
  CASE -- adresseert standplaats
    WHEN prs."IdentificatiecodeVerblijfplaats" IS NULL THEN NULL
    WHEN substring(prs."IdentificatiecodeVerblijfplaats", 5, 2) = '03'
      THEN prs."IdentificatiecodeVerblijfplaats"
    ELSE NULL
  END                                                                              AS adresseert_bag_standplaats,
  prs."GemeenteVanInschrijvingOms"                                                 AS gemeente_van_inschrijving,
  brp_datum_prefix_prs."DatumInschrijving"                                         AS datum_inschrijving,
  prs."FunctieAdres"                                                               AS functie_adres,
  NULL::varchar                                                                    AS gemeentedeel, -- Not available
  brp_datum_prefix_prs."DatumAanvangHuishouding"                                   AS datum_aanvang_adreshouding,
  prs."Straatnaam"                                                                 AS straatnaam,
  prs."NaamOpenbareRuimte"                                                         AS naam_openbare_ruimte,
  prs."Huisnummer"                                                                 AS huisnummer,
  prs."Huisletter"                                                                 AS huisletter,
  prs."Huisnummertoevoeging"                                                       AS huisnummertoevoeging,
  prs."AanduidingBijHuisnummer"                                                    AS aanduiding_bij_huisnummer,
  prs."Postcode"                                                                   AS postcode,
  prs."Woonplaatsnaam"                                                             AS woonplaatsnaam,
  prs."IdentificatiecodeNummeraanduiding"                                          AS heeft_bag_hoofdadres,
  prs."Locatiebeschrijving"                                                        AS locatiebeschrijving,
  JSONB_BUILD_OBJECT( -- land van waar ingeschreven
    'code', NULL::varchar,  -- Not available
    'omschrijving', NULL::varchar  -- Not available
  )                                                                                AS land_waarnaar_vertrokken,
  NULL::varchar::date                                                              AS datum_vertrek_uit_nederland,
  prs."AdresBuitenland1"                                                           AS adres_buitenland_waarnaar_vertrokken1,
  prs."AdresBuitenland1"                                                           AS adres_buitenland_waarnaar_vertrokken2,
  prs."AdresBuitenland1"                                                           AS adres_buitenland_waarnaar_vertrokken3,
  JSONB_BUILD_OBJECT( -- land van waar ingeschreven
    'code', prs."LandVanwaarIngeschrevenCode"::varchar,
    'omschrijving', prs."LandVanwaarIngeschrevenOms"::varchar
  )                                                                                AS land_vanwaar_ingeschreven,
  prs."DatumVestigingInNederland"                                                  AS datum_vestiging_nederland,
  NULL                                                                             AS aangever_adreshouding, -- Not available
  NULL                                                                             AS indicatie_document, -- Not available

  prs."GegevensInOnderzoekAdres"::varchar                                          AS aanduiding_gegevens_in_onderzoek,
  brp_datum_prefix_prs."DatumIngangOnderzoekAdres"                                 AS datum_ingang_onderzoek,
  brp_datum_prefix_prs."DatumEindeOnderzoekAdres"                                  AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  brp_datum_prefix_prs."DatumGeldigheidAdres"                                      AS ingangsdatum_geldigheid,
  brp_datum_prefix_prs."DatumOpnameAdres"                                          AS datum_opneming,
  NULL                                                                             AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.personen prs
