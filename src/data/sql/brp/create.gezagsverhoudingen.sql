SELECT
  gv."BSN"::varchar                                                                AS burgerservicenummer,
  gv."Anummer"::varchar                                                            AS anummer,
  gv."IndGezagMinderjarigeOms"::varchar                                            AS indicatie_gezag_minderjarige,
  gv."IndCurateleRegisterOms"                                                      AS indicatie_curatele_register, -- TODO: Check this
  gv."GemeenteOntleningCode"::varchar                                              AS gemeente_gezagsverhouding_document,
  brp_build_date_json(gv."DatumOntlening")                                         AS datum_ontlening_gezagsverhouding,
  gv."BeschrijvingDocument"                                                        AS beschrijving_document_gezagsverouding,
  gv."GegevensInOnderzoek"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  brp_build_date_json(gv."DatumIngangOnderzoek")                                   AS datum_ingang_onderzoek,
  brp_build_date_json(gv."DatumEindeOnderzoek")                                    AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,
  brp_build_date_json(gv."DatumGeldigheid")                                        AS ingangsdatum_geldigheid,
  brp_build_date_json(gv."DatumOpname")                                            AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.gezagsverhouding gv
