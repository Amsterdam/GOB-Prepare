SELECT
  vt."BSN"::varchar                                                                AS burgerservicenummer,
  vt."Anummer"::varchar                                                            AS anummer,
  JSONB_BUILD_OBJECT( -- nationaliteit
    'code', vt."VerblijftitelCode"::varchar,
    'omschrijving', vt."VerblijftitelOms"::varchar
  )                                                                                AS aanduiding_verblijfstitel,
  brp_datum_prefix_vt."DatumVerkrijging"                                           AS ingangsdatum_verblijfstitel,

  brp_datum_prefix_vt."DatumVerlies"                                               AS datum_einde_verblijfstitel,

  vt."GegevensInOnderzoek"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  brp_datum_prefix_vt."DatumIngangOnderzoek"                                       AS datum_ingang_onderzoek,
  brp_datum_prefix_vt."DatumEindeOnderzoek"                                        AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  brp_datum_prefix_vt."DatumGeldigheid"                                            AS ingangsdatum_geldigheid,
  brp_datum_prefix_vt."DatumOpname"                                                AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- still have to decide what will be

FROM brp.verblijfstitel vt
