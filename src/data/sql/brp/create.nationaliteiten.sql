SELECT
  nat."BSN"::varchar                                                               AS burgerservicenummer,
  nat."Anummer"::varchar                                                           AS anummer,
  JSONB_BUILD_OBJECT( -- nationaliteit
    'code', nat."NationaliteitCode"::varchar,
    'omschrijving', nat."NationaliteitOms"::varchar
    )                                                                              AS nationaliteit,
  JSONB_BUILD_OBJECT( -- nationaliteit verkrijging
    'code', nat."VerkrijgingCode"::varchar,
    'omschrijving', nat."VerkrijgingOms"::varchar
    )                                                                              AS reden_verkrijging_ned_nat,
  JSONB_BUILD_OBJECT( -- nationaliteit verlies
    'code', nat."VerliesCode"::varchar,
    'omschrijving', nat."VerliesOms"::varchar
    )                                                                              AS reden_verlies_ned_nat,
  nat."ByzNederlanderschapOms"::varchar                                            AS aanduiding_bijzonder_nederlanderschap,
  nat."GemeenteCode"::varchar                                                      AS gemeente_document,
  CASE -- datum document
    WHEN nat."Datumdocument" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(nat."Datumdocument", 1, 4),
          substring(nat."Datumdocument", 5, 2),
          substring(nat."Datumdocument", 7, 2)
        ),
      'jaar', substring(nat."Datumdocument", 1, 4),
      'maand', substring(nat."Datumdocument", 5, 2),
      'dag', substring(nat."Datumdocument", 7, 2)
      )
  END                                                                              AS datum_document,
  nat."DocumentOms"::varchar                                                       AS beschrijving_document,

  nat."GegevensInOnderzoek"::varchar                                               AS aanduiding_gegevens_in_onderzoek,
  CASE -- datum ingang onderzoek
    WHEN nat."DatumIngangOnderzoek" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(nat."DatumIngangOnderzoek", 1, 4),
        substring(nat."DatumIngangOnderzoek", 5, 2),
        substring(nat."DatumIngangOnderzoek", 7, 2)
      ),
      'jaar', substring(nat."DatumIngangOnderzoek", 1, 4),
      'maand', substring(nat."DatumIngangOnderzoek", 5, 2),
      'dag', substring(nat."DatumIngangOnderzoek", 7, 2)
    )
  END                                                                              AS datum_ingang_onderzoek,

  CASE -- datum einde onderzoek
    WHEN nat."DatumEindeOnderzoek" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(nat."DatumEindeOnderzoek", 1, 4),
        substring(nat."DatumEindeOnderzoek", 5, 2),
        substring(nat."DatumEindeOnderzoek", 7, 2)
      ),
      'jaar', substring(nat."DatumEindeOnderzoek", 1, 4),
      'maand', substring(nat."DatumEindeOnderzoek", 5, 2),
      'dag', substring(nat."DatumEindeOnderzoek", 7, 2)
    )
  END                                                                              AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  CASE -- datum geldigheid
    WHEN nat."DatumGeldigheid" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(nat."DatumGeldigheid", 1, 4),
          substring(nat."DatumGeldigheid", 5, 2),
          substring(nat."DatumGeldigheid", 7, 2)
        ),
      'jaar', substring(nat."DatumGeldigheid", 1, 4),
      'maand', substring(nat."DatumGeldigheid", 5, 2),
      'dag', substring(nat."DatumGeldigheid", 7, 2)
      )
  END                                                                              AS ingangsdatum_geldigheid,
  CASE -- datum opneming
    WHEN nat."DatumOpname" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(nat."DatumOpname", 1, 4),
          substring(nat."DatumOpname", 5, 2),
          substring(nat."DatumOpname", 7, 2)
        ),
      'jaar', substring(nat."DatumOpname", 1, 4),
      'maand', substring(nat."DatumOpname", 5, 2),
      'dag', substring(nat."DatumOpname", 7, 2)
      )
  END                                                                              AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- still have to decide what will be

FROM brp.nationaliteiten nat
