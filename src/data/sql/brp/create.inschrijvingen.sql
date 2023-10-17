SELECT
  prs."BSN"::varchar                                                     AS burgerservicenummer,
  prs."Anummer"::varchar                                                 AS anummer,
  NULL::varchar::date                                                    AS datum_ingang_blokkering_pL,
  NULL::varchar::date                                                    AS datum_opschorting_bijhouding,
  NULL::varchar                                                          AS omschrijving_reden_opschorting_bijhouding,
  brp_build_date_json(prs."DatumInschrijving")                           AS datum_eerste_inschrijving_gba,
  prs."GemeenteVanInschrijvingCode"::varchar                             AS gemeente_waar_persoonskaart_is,
  JSONB_BUILD_OBJECT( -- indicatie geheim
    'code', prs."IndGeheimCode"::varchar,
    'omschrijving', prs."IndGeheimOms"::varchar
  )                                                                      AS indicatie_geheim,
  NULL                                                                   AS persoonskaart_gegevens_volledig_meegeconverteerd,
  brp_build_date_json(prs."DatumGeldigheidAdres")                        AS ingangsdatum_geldigheid,
  brp_build_date_json(prs."DatumOpnameAdres")                            AS datum_opneming,
  NULL::varchar::date                                                    AS datum_actueel_tot -- still have to decide what will be

FROM brp.personen prs
