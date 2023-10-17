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
  brp_build_date_json(nat."Datumdocument")                                         AS datum_document,
  nat."DocumentOms"::varchar                                                       AS beschrijving_document,
  nat."GegevensInOnderzoek"::varchar                                               AS aanduiding_gegevens_in_onderzoek,
  brp_build_date_json(nat."DatumIngangOnderzoek")                                  AS datum_ingang_onderzoek,
  brp_build_date_json(nat."DatumEindeOnderzoek")                                   AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,
  brp_build_date_json(nat."DatumGeldigheid")                                       AS ingangsdatum_geldigheid,
  brp_build_date_json(nat."DatumOpname")                                           AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- still have to decide what will be

FROM brp.nationaliteiten nat
