CREATE TABLE brp_prep.inschrijvingen AS

  SELECT
    prs."BSN"::varchar                                                     AS burgerservicenummer,
    prs."Anummer"::varchar                                                 AS anummer,
    NULL::varchar::date                                                    AS datum_ingang_blokkering_pL,
    NULL::varchar::date                                                    AS datum_opschorting_bijhouding,
    NULL::text                                                             AS omschrijving_reden_opschorting_bijhouding,
    CASE -- datum eerste inschrijving
      WHEN prs."DatumInschrijving" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(prs."DatumInschrijving", 1, 4),
            substring(prs."DatumInschrijving", 5, 2),
            substring(prs."DatumInschrijving", 7, 2)
          ),
        'jaar', substring(prs."DatumInschrijving", 1, 4),
        'maand', substring(prs."DatumInschrijving", 5, 2),
        'dag', substring(prs."DatumInschrijving", 7, 2)
        )
    END                                                                    AS datum_eerste_inschrijving_gba,
    prs."GemeenteVanInschrijvingCode"::varchar                             AS gemeente_waar_persoonskaart_is,
    JSONB_BUILD_OBJECT( -- indicatie geheim
      'code', prs."IndGeheimCode"::varchar,
      'omschrijving', prs."IndGeheimOms"::text
    )                                                                      AS indicatie_geheim,
    NULL                                                                   AS persoonskaart_gegevens_volledig_meegeconverteerd,
    NULL::varchar::date                                                    AS datum_actueel_tot -- still have to decide what will be

  FROM brp.personen prs
