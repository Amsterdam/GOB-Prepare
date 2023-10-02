CREATE TABLE brp_prep.verblijfplaatsen AS

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
      WHEN prs."DatumInschrijving" = '0'
        OR prs."DatumInschrijving" = '00000000' THEN '0000-00-00'
      WHEN length(prs."DatumInschrijving") = 8
        AND prs."DatumInschrijving" != '00000000' THEN CONCAT_WS(
          '-',
          substring(prs."DatumInschrijving", 1, 4),
          substring(prs."DatumInschrijving", 5, 2),
          substring(prs."DatumInschrijving", 7, 2)
        )
      ELSE prs."DatumInschrijving"
    END                                                                              AS datum_inschrijving,
    prs."FunctieAdres"                                                               AS functie_adres,
    NULL::varchar                                                                    AS gemeentedeel, -- Not available
    CASE -- datum inschrijving
      WHEN prs."DatumAanvangHuishouding" IS NULL THEN NULL
      WHEN prs."DatumAanvangHuishouding" = '0'
        OR prs."DatumAanvangHuishouding" = '00000000' THEN '0000-00-00'
      WHEN length(prs."DatumAanvangHuishouding") = 8
        AND prs."DatumAanvangHuishouding" != '00000000' THEN CONCAT_WS(
          '-',
          substring(prs."DatumAanvangHuishouding", 1, 4),
          substring(prs."DatumAanvangHuishouding", 5, 2),
          substring(prs."DatumAanvangHuishouding", 7, 2)
        )
      ELSE prs."DatumAanvangHuishouding"
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
    JSONB_BUILD_OBJECT( -- onderzoek 
      'aanduiding_gegevens_in_onderzoek', prs."GegevensInOnderzoekAdres"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek
          WHEN prs."DatumIngangOnderzoekAdres" IS NULL THEN NULL
          WHEN prs."DatumIngangOnderzoekAdres" = '0'
            OR prs."DatumIngangOnderzoekAdres" = '00000000' THEN '0000-00-00'
          WHEN length(prs."DatumIngangOnderzoekAdres") = 8
            AND prs."DatumIngangOnderzoekAdres" != '00000000' THEN CONCAT(
               '_',
              substring(prs."DatumIngangOnderzoekAdres", 1, 4),
              substring(prs."DatumIngangOnderzoekAdres", 5, 2),
              substring(prs."DatumIngangOnderzoekAdres", 7, 2)
            )
          ELSE prs."DatumIngangOnderzoekAdres"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek
          WHEN prs."DatumEindeOnderzoekAdres" IS NULL THEN NULL
          WHEN prs."DatumEindeOnderzoekAdres" = '0'
            OR prs."DatumEindeOnderzoekAdres" = '00000000' THEN '0000-00-00'
          WHEN length(prs."DatumEindeOnderzoekAdres") = 8
            AND prs."DatumEindeOnderzoekAdres" != '00000000' THEN CONCAT_WS(
              '-',
              substring(prs."DatumEindeOnderzoekAdres", 1, 4),
              substring(prs."DatumEindeOnderzoekAdres", 5, 2),
              substring(prs."DatumEindeOnderzoekAdres", 7, 2)
            )
          ELSE prs."DatumEindeOnderzoekAdres"::varchar
        END,
      'onjuist_strijdig_openbare_orde', NULL::varchar -- Not available
    )                                                                                AS onderzoek,
    CASE -- ingangsdatum geldigheid
      WHEN prs."DatumGeldigheidAdres" IS NULL THEN NULL
      WHEN prs."DatumGeldigheidAdres" = '0'
        OR prs."DatumGeldigheidAdres" = '00000000' THEN '0000-00-00'
      WHEN length(prs."DatumGeldigheidAdres") = 8
        AND prs."DatumGeldigheidAdres" != '00000000' THEN CONCAT_WS(
          '-',
          substring(prs."DatumGeldigheidAdres", 1, 4),
          substring(prs."DatumGeldigheidAdres", 5, 2),
          substring(prs."DatumGeldigheidAdres", 7, 2)
        )
      ELSE prs."DatumGeldigheidAdres"
    END                                                                              AS ingangsdatum_geldigheid,
    CASE -- datum opneming
      WHEN prs."DatumOpnameAdres" IS NULL THEN NULL
      WHEN prs."DatumOpnameAdres" = '0'
        OR prs."DatumOpnameAdres" = '00000000' THEN '0000-00-00'
      WHEN length(prs."DatumOpnameAdres") = 8
        AND prs."DatumOpnameAdres" != '00000000' THEN CONCAT_WS(
          '-',
          substring(prs."DatumOpnameAdres", 1, 4),
          substring(prs."DatumOpnameAdres", 5, 2),
          substring(prs."DatumOpnameAdres", 7, 2)
        )
      ELSE prs."DatumOpnameAdres"
    END                                                                              AS datum_opneming,
    NULL                                                                             AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.personen prs
