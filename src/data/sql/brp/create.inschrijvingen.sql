CREATE TABLE brp_prep.inschrijvingen AS

  SELECT
    prs."BSN"::varchar                                                    AS burgerservicenummer,
    prs."Anummer"::varchar                                                AS anummer,
    NULL::varchar::date                                                   AS datum_ingang_blokkering_pL,
    NULL::varchar::date                                                   AS datum_opschorting_bijhouding,
    NULL::text                                                            AS omschrijving_reden_opschorting_bijhouding,
    CASE -- datum eerste inschrijving
      WHEN prs."DatumInschrijving" IS NULL THEN NULL
      WHEN prs."DatumInschrijving" = '0' THEN '0000-00-00'
      WHEN length(prs."DatumInschrijving") = 8 THEN CONCAT_WS(
          '-',
          substring(prs."DatumInschrijving", 1, 4),
          substring(prs."DatumInschrijving", 5, 2),
          substring(prs."DatumInschrijving", 7, 2)
        )
      ELSE prs."DatumInschrijving"
    END                                                                    AS datum_eerste_inschrijving_gba,
    prs."GemeenteVanInschrijvingCode"::varchar                            AS gemeente_waar_persoonskaart_is,
    JSONB_BUILD_OBJECT( -- nationaliteit
      'code', prs."IndGeheimCode"::varchar,
      'omschrijving', prs."IndGeheimOms"::text
    )                                                                      AS indicatie_geheim,
    NULL                                                                   AS persoonskaart_gegevens_volledig_meegeconverteerd,
    NULL::varchar::date                                                    AS datum_actueel_tot -- still have to decide what will be

  FROM brp.personen prs
