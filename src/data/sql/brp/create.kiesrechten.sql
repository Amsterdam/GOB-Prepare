SELECT
  kr."BSN"::varchar                                                                AS burgerservicenummer,
  kr."Anummer"::varchar                                                            AS anummer,
  kr."AandEuropeesKiesrecht"::varchar                                              AS aand_euro_kiesrecht,

  brp_datum_prefix_kr."DatumVerzoekEukiesrecht"                                    AS datum_euro_kiesrecht,
  brp_datum_prefix_kr."DatumEindeUitsluitingEuKiesrecht"                           AS einddatum_euro_kiesrecht,
  NULL::varchar                                                                    AS adres_e_ulidstaat_herkomst,  -- deze worden in toekomst geleverd
  NULL::varchar                                                                    AS plaats_e_ulidstaat_herkomst, -- deze worden in toekomst geleverd
  NULL::varchar                                                                    AS land_e_ulidstaat_herkomst,   -- deze worden in toekomst geleverd
  kr."AandUitsluitingKiesrecht"::varchar                                           AS aand_uitgesloten_kiesrecht,

  brp_datum_prefix_kr."DatumEindeUitsluitingKiesrecht"                             AS einddatum_uitsluiting_kiesrecht,
  kr."GemeenteCodeOntlening"::varchar                                              AS gemeente_document,

  brp_datum_prefix_kr."DatumOntlening"                                             AS datum_document,
  kr."BeschrijvingDocument"::varchar                                               AS beschrijving_document,

  -- TODO: this 4 feature are in the mapping specified but unvailable in src data.
  NULL::varchar                                                                    AS aanduiding_gegevens_in_onderzoek,
  NULL::date                                                                       AS datum_ingang_onderzoek,
  NULL::date                                                                       AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  NULL::date                                                                        AS ingangsdatum_geldigheid, -- TODO: in the mapping but unvailable in src data.
  NULL::date                                                                        AS datum_opneming, -- TODO: in the mapping but unvailable in src data.
  NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.kiesrecht kr
