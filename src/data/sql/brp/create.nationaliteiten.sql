CREATE TABLE brp_prep.nationaliteiten AS

  SELECT
    nat."BSN"::varchar                                                               AS burgerservicenummer,
    nat."Anummer"::varchar                                                           AS anummer,
    JSONB_BUILD_OBJECT( -- nationaliteit
      'code', nat."NationaliteitCode"::varchar,
      'omschrijving', nat."NationaliteitOms"::text
      )                                                                              AS nationaliteit,
    JSONB_BUILD_OBJECT( -- nationaliteit verkrijging
      'code', nat."VerkrijgingCode"::varchar,
      'omschrijving', nat."VerkrijgingOms"::text
      )                                                                              AS reden_verkrijging_ned_nat,
    JSONB_BUILD_OBJECT( -- nationaliteit verlies
      'code', nat."VerliesCode"::varchar,
      'omschrijving', nat."VerliesOms"::text
      )                                                                              AS reden_verlies_ned_nat,
    nat."ByzNederlanderschap"::text                                                  AS aanduiding_bijzonder_nederlanderschap,
    nat."GemeenteCode"::varchar                                                      AS gemeente_document,
    CASE -- datum document
      WHEN nat."Datumdocument" IS NULL THEN NULL
      WHEN nat."Datumdocument" = '0'
        OR nat."Datumdocument" = '00000000' THEN '0000-00-00'
      WHEN length(nat."Datumdocument") = 8
        AND nat."Datumdocument" != '00000000' THEN CONCAT_WS(
          '-',
          substring(nat."Datumdocument", 1, 4),
          substring(nat."Datumdocument", 5, 2),
          substring(nat."Datumdocument", 7, 2)
        )
      ELSE nat."Datumdocument"
    END                                                                              AS datum_document,
    nat."DocumentOms"::text                                                          AS beschrijving_document,
    JSONB_BUILD_OBJECT( -- onderzoek 
      'aanduiding_gegevens_in_onderzoek', nat."GegevensInOnderzoek"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek
          WHEN nat."DatumIngangOnderzoek" IS NULL THEN NULL
          WHEN nat."DatumIngangOnderzoek" = '0'
            OR nat."DatumIngangOnderzoek" = '00000000' THEN '0000-00-00'
          WHEN length(nat."DatumIngangOnderzoek") = 8
            AND nat."DatumIngangOnderzoek" != '00000000' THEN CONCAT_WS(
              '-',
              substring(nat."DatumIngangOnderzoek", 1, 4),
              substring(nat."DatumIngangOnderzoek", 5, 2),
              substring(nat."DatumIngangOnderzoek", 7, 2)
            )
          ELSE nat."DatumIngangOnderzoek"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek
          WHEN nat."DatumEindeOnderzoek" IS NULL THEN NULL
          WHEN nat."DatumEindeOnderzoek" = '0'
            OR nat."DatumEindeOnderzoek" = '00000000' THEN '0000-00-00'
          WHEN length(nat."DatumEindeOnderzoek") = 8
            AND nat."DatumEindeOnderzoek" != '00000000' THEN CONCAT_WS(
              '-',
              substring(nat."DatumEindeOnderzoek", 1, 4),
              substring(nat."DatumEindeOnderzoek", 5, 2),
              substring(nat."DatumEindeOnderzoek", 7, 2)
            )
          ELSE nat."DatumEindeOnderzoek"::varchar
        END,
      'onjuist_strijdig_openbare_orde', NULL::text
    )                                                                                AS onderzoek,
    CASE -- datum geldigheid
      WHEN nat."DatumGeldigheid" IS NULL THEN NULL
      WHEN nat."DatumGeldigheid" = '0'
        OR nat."DatumGeldigheid" = '00000000' THEN '0000-00-00'
      WHEN length(nat."DatumGeldigheid") = 8
        AND nat."DatumGeldigheid" != '00000000' THEN CONCAT_WS(
          '-',
          substring(nat."DatumGeldigheid", 1, 4),
          substring(nat."DatumGeldigheid", 5, 2),
          substring(nat."DatumGeldigheid", 7, 2)
        )
      ELSE nat."DatumGeldigheid"
    END                                                                              AS ingangsdatum_geldigheid,
    CASE -- datum opneming
      WHEN nat."DatumOpname" IS NULL THEN NULL
      WHEN nat."DatumOpname" = '0'
        OR nat."DatumOpname" = '00000000' THEN '0000-00-00'
      WHEN length(nat."DatumOpname") = 8
        AND nat."DatumOpname" != '00000000' THEN CONCAT_WS(
          '-',
          substring(nat."DatumOpname", 1, 4),
          substring(nat."DatumOpname", 5, 2),
          substring(nat."DatumOpname", 7, 2)
        )
      ELSE nat."DatumOpname"
    END                                                                              AS datum_opneming,
    NULL::varchar::date                                                              AS datum_actueel_tot -- still have to decide what will be

  FROM brp.nationaliteiten nat
