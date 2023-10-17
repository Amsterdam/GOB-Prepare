SELECT
  prs."BSN"::varchar                                                                       AS burgerservicenummer,
  prs."Anummer"::varchar                                                                   AS a_nummer,
  prs."Geslachtsnaam"::varchar                                                             AS geslachtsnaam,
  JSONB_BUILD_OBJECT( -- Voorvoegsel 
      'code', NULL,
      'omschrijving', prs."Voorvoegsel"::varchar
    )                                                                                      AS voorvoegsel_geslachtsnaam,
  prs."Voornamen"::varchar                                                                 AS voornamen,
  CASE -- Adellijketitel 
    WHEN prs."Adellijketitel" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'code', NULL,
      'omschrijving', prs."Adellijketitel"::varchar
  )
  END                                                                                      AS adellijke_titel_redikaat,
  prs."GeboorteplaatsOms"::varchar                                                         AS geboorte_plaats,
  prs."GeboortelandOms"::varchar                                                           AS geboorte_land,
  brp_datum_prefix_prs."Geboortedatum"                                                     AS geboortedatum,
  prs."GeslachtsaanduidingOms"::varchar                                                    AS geslachtsaanduiding,
  NULL                                                                                     AS vorig_a_nummer,
  NULL                                                                                     AS volgende_a_nummer,
  prs."AanduidingNaamgebruik"::varchar                                                     AS naamgebruik,
  JSONB_BUILD_OBJECT( -- registergemeente akte
    'code', NULL::varchar
  )                                                                                         AS registergemeente_akte,
  JSONB_BUILD_OBJECT(
      'code', NULL,
      'omschrijving', NULL
  )                                                                                        AS aktenummer, 
  prs."GemeenteVanInschrijvingCode"::varchar                                               AS gemeente_document,
  NULL                                                                                     AS datum_document,
  NULL                                                                                     AS beschrijving_document,

  prs."GegevensInOnderzoekPersoon"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  brp_datum_prefix_prs."DatumIngangOnderzoekPersoon"                                       AS datum_ingang_onderzoek,
  brp_datum_prefix_prs."DatumEindeOnderzoekPersoon"                                        AS datum_einde_onderzoek,
  NULL::varchar                                                                            AS onjuist_strijdig_openbare_orde,

  brp_datum_prefix_prs."DatumGeldigheidPersoon"                                            AS begin_geldigheid,
  NULL::date                                                                               AS eind_geldigheid,
  brp_datum_prefix_prs."DatumOpnamePersoon"                                                AS datum_opneming,

  prs."BSNOuder1"                                                                          AS heeft_brp_ouder1,
  prs."BSNOuder2"                                                                          AS heeft_brp_ouder2,
  nat.nationaliteiten                                                                      AS heeft_brp_nationalteiten,
  hw.huwelijken                                                                            AS heeft_brp_huwelijk_partnerschap,
  ov.overlijden                                                                            AS heeft_brp_overlijden, -- TODO: overlijdens table is not completed
  JSONB_BUILD_OBJECT(
    'burgerservicenummer', prs."BSN"
  )                                                                                        AS heeft_brp_inschrijving,
  JSONB_BUILD_OBJECT(
    'burgerservicenummer', prs."BSN"
  )                                                                                        AS heeft_brp_verblijfplaats,
  prs."BSN"                                                                                AS heeft_brp_kind,
  vt.verblijfstitel                                                                        AS heeft_brp_verblijfstitel,
  gv.gezagsverhouding                                                                      AS heeft_brp_gezagsverhouding,
  rd.reisdocumenten                                                                        AS heeft_brp_reisdocument,
  verw.verwijzingen                                                                        AS heeft_brp_verwijzing,
  kr.kiesrechten                                                                           AS heeft_brp_kiesrecht,
  NULL                                                                                     AS datum_actueel_tot -- still have to decide what will be

FROM
  brp.personen prs

  LEFT JOIN ( -- nationaliteiten
    SELECT 
      "BSN",
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'burgerservicenummer', "BSN"
        )
      ) AS nationaliteiten
    FROM brp.nationaliteiten
    GROUP BY
      "BSN"
  ) AS nat ON prs."BSN" = nat."BSN"

  LEFT JOIN ( -- verblijfstitel
    SELECT 
      "BSN",
      JSONB_BUILD_OBJECT(
        'burgerservicenummer', "BSN"
      ) AS verblijfstitel
    FROM brp.verblijfstitel
    ) AS vt ON prs."BSN" = vt."BSN"

  LEFT JOIN ( -- huwelijkenpartnerschappen
    SELECT 
      "BSN",
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'burgerservicenummer', "BSN"
        )
      ) AS huwelijken
    FROM brp.huwelijk
    GROUP BY
      "BSN"
  ) AS hw ON prs."BSN" = hw."BSN"

  LEFT JOIN ( -- gezagsverhouding
    SELECT 
      "BSN",
      JSONB_BUILD_OBJECT(
        'burgerservicenummer', "BSN"
      ) AS gezagsverhouding
    FROM brp.gezagsverhouding
  ) AS gv ON prs."BSN" = gv."BSN"

  LEFT JOIN  ( -- verwijzingen
    SELECT 
      "BSN",
      JSONB_BUILD_OBJECT(
        'burgerservicenummer', "BSN"
      ) AS verwijzingen
    FROM brp.verwijsgegevens
  ) AS verw ON prs."BSN" = verw."BSN"

  LEFT JOIN  ( -- kiesrechten
    SELECT 
      "BSN",
      JSONB_BUILD_OBJECT(
        'burgerservicenummer', "BSN"
      ) AS kiesrechten
    FROM brp.kiesrecht
  ) AS kr ON prs."BSN" = kr."BSN"

  LEFT JOIN  ( -- reisdocumenten
    SELECT 
      "BSN",
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'burgerservicenummer', "BSN"
        )
      ) AS reisdocumenten
    FROM brp.reisdocumenten
    GROUP BY
      "BSN"
  ) AS rd ON prs."BSN" = rd."BSN"

  LEFT JOIN  ( -- overlijdens
    SELECT 
      "BSN",
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'burgerservicenummer', "BSN"
        )
      ) AS overlijden
    FROM brp.overlijden
    GROUP BY
      "BSN"
  ) AS ov ON prs."BSN" = ov."BSN"
