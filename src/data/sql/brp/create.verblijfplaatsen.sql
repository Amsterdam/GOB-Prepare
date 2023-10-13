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
  CASE -- datum inschrijving
    WHEN prs."DatumInschrijving" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(prs."DatumInschrijving", 1, 4),
          substring(prs."DatumInschrijving", 5, 2),
          substring(prs."DatumInschrijving", 7, 2)
        ),
      'jaar', substring(prs."DatumInschrijving", 1, 4),
      'maand', substring(prs."DatumInschrijving", 5, 2),
      'dag', substring(prs."DatumInschrijving", 7, 2)
      )
  END                                                                              AS datum_inschrijving,
  prs."FunctieAdres"                                                               AS functie_adres,
  NULL::varchar                                                                    AS gemeentedeel, -- Not available
  CASE -- datum inschrijving
    WHEN prs."DatumAanvangHuishouding" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(prs."DatumAanvangHuishouding", 1, 4),
          substring(prs."DatumAanvangHuishouding", 5, 2),
          substring(prs."DatumAanvangHuishouding", 7, 2)
        ),
      'jaar', substring(prs."DatumAanvangHuishouding", 1, 4),
      'maand', substring(prs."DatumAanvangHuishouding", 5, 2),
      'dag', substring(prs."DatumAanvangHuishouding", 7, 2)
      )
  END                                                                              AS datum_aanvang_adreshouding,
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

  prs."GegevensInOnderzoekAdres"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  CASE -- datum ingang onderzoek
    WHEN prs."DatumIngangOnderzoekAdres" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(prs."DatumIngangOnderzoekAdres", 1, 4),
        substring(prs."DatumIngangOnderzoekAdres", 5, 2),
        substring(prs."DatumIngangOnderzoekAdres", 7, 2)
      ),
      'jaar', substring(prs."DatumIngangOnderzoekAdres", 1, 4),
      'maand', substring(prs."DatumIngangOnderzoekAdres", 5, 2),
      'dag', substring(prs."DatumIngangOnderzoekAdres", 7, 2)
    )
  END                                                                              AS datum_ingang_onderzoek,

  CASE -- datum einde onderzoek
    WHEN prs."DatumEindeOnderzoekAdres" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(prs."DatumEindeOnderzoekAdres", 1, 4),
        substring(prs."DatumEindeOnderzoekAdres", 5, 2),
        substring(prs."DatumEindeOnderzoekAdres", 7, 2)
      ),
      'jaar', substring(prs."DatumEindeOnderzoekAdres", 1, 4),
      'maand', substring(prs."DatumEindeOnderzoekAdres", 5, 2),
      'dag', substring(prs."DatumEindeOnderzoekAdres", 7, 2)
    )
  END                                                                              AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  CASE -- ingangsdatum geldigheid
    WHEN prs."DatumGeldigheidAdres" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(prs."DatumGeldigheidAdres", 1, 4),
          substring(prs."DatumGeldigheidAdres", 5, 2),
          substring(prs."DatumGeldigheidAdres", 7, 2)
        ),
      'jaar', substring(prs."DatumGeldigheidAdres", 1, 4),
      'maand', substring(prs."DatumGeldigheidAdres", 5, 2),
      'dag', substring(prs."DatumGeldigheidAdres", 7, 2)
      )
  END                                                                              AS ingangsdatum_geldigheid,
  CASE -- datum opneming
    WHEN prs."DatumOpnameAdres" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(prs."DatumOpnameAdres", 1, 4),
          substring(prs."DatumOpnameAdres", 5, 2),
          substring(prs."DatumOpnameAdres", 7, 2)
        ),
      'jaar', substring(prs."DatumOpnameAdres", 1, 4),
      'maand', substring(prs."DatumOpnameAdres", 5, 2),
      'dag', substring(prs."DatumOpnameAdres", 7, 2)
      )
  END                                                                              AS datum_opneming,
  NULL                                                                             AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.personen prs
