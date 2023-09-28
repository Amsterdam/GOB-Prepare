CREATE TABLE brp_prep.inschrijvingen AS

  SELECT
    pers."BSN"::varchar                                                    AS burgerservicenummer,
    pers."Anummer"::varchar                                                AS anummer,
    NULL::text::date                                                       AS datum_ingang_blokkering_pL,
    NULL::text::date                                                       AS datum_opschorting_bijhouding,
    NULL::text::varchar                                                    AS omschrijving_reden_opschorting_bijhouding,
    CASE -- datum eerste inschrijving
      WHEN pers."DatumInschrijving" IS NULL THEN NULL
      WHEN pers."DatumInschrijving" = '0' THEN '0000-00-00'
      WHEN length(pers."DatumInschrijving") = 8
        AND pers."DatumInschrijving" != '00000000' THEN CONCAT(
          substring(pers."DatumInschrijving", 1, 4),
          '-',
          substring(pers."DatumInschrijving", 5, 2),
          '-',
          substring(pers."DatumInschrijving", 7, 2)
        )
      ELSE pers."DatumInschrijving"
    END                                                                    AS datum_eerste_inschrijving_gba,
    pers."GemeenteVanInschrijvingCode"::varchar                            AS gemeente_waar_persoonskaart_is,
    JSONB_BUILD_OBJECT( -- nationaliteit
      'code', pers."IndGeheimCode"::varchar,
      'omschrijving', pers."IndGeheimOms"::text
    )                                                                      AS indicatie_geheim,
    NULL                                                                   AS persoonskaart_gegevens_volledig_meegeconverteerd,
    NULL                                                                   AS datum_actueel_tot -- still have to decide what will be

  FROM brp.personen pers