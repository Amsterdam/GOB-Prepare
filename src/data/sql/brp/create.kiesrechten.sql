CREATE TABLE brp_prep.kiesrechten AS

  SELECT
    kr."BSN"::varchar                                                                AS burgerservicenummer,
    kr."Anummer"::varchar                                                            AS anummer,
    kr."AandEuropeesKiesrecht"::varchar                                              AS aand_euro_kiesrecht,
    CASE -- datum eu kiesrecht
      WHEN kr."DatumVerzoekEukiesrecht" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kr."DatumVerzoekEukiesrecht", 1, 4),
            substring(kr."DatumVerzoekEukiesrecht", 5, 2),
            substring(kr."DatumVerzoekEukiesrecht", 7, 2)
          ),
        'jaar', substring(kr."DatumVerzoekEukiesrecht", 1, 4),
        'maand', substring(kr."DatumVerzoekEukiesrecht", 5, 2),
        'dag', substring(kr."DatumVerzoekEukiesrecht", 7, 2)
        )
    END                                                                              AS datum_euro_kiesrecht,
    CASE -- einddatum eu kiesrecht
      WHEN kr."DatumEindeUitsluitingEuKiesrecht" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kr."DatumEindeUitsluitingEuKiesrecht", 1, 4),
            substring(kr."DatumEindeUitsluitingEuKiesrecht", 5, 2),
            substring(kr."DatumEindeUitsluitingEuKiesrecht", 7, 2)
          ),
        'jaar', substring(kr."DatumEindeUitsluitingEuKiesrecht", 1, 4),
        'maand', substring(kr."DatumEindeUitsluitingEuKiesrecht", 5, 2),
        'dag', substring(kr."DatumEindeUitsluitingEuKiesrecht", 7, 2)
        )
    END                                                                              AS einddatum_euro_kiesrecht,
    NULL::varchar                                                                    AS adres_e_ulidstaat_herkomst,  -- deze worden in toekomst geleverd
    NULL::varchar                                                                    AS plaats_e_ulidstaat_herkomst, -- deze worden in toekomst geleverd
    NULL::varchar                                                                    AS land_e_ulidstaat_herkomst,   -- deze worden in toekomst geleverd
    kr."AandUitsluitingKiesrecht"::varchar                                           AS aand_uitgesloten_kiesrecht,
    CASE -- einddatum uitsluiting kiesrecht
      WHEN kr."DatumEindeUitsluitingKiesrecht" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kr."DatumEindeUitsluitingKiesrecht", 1, 4),
            substring(kr."DatumEindeUitsluitingKiesrecht", 5, 2),
            substring(kr."DatumEindeUitsluitingKiesrecht", 7, 2)
          ),
        'jaar', substring(kr."DatumEindeUitsluitingKiesrecht", 1, 4),
        'maand', substring(kr."DatumEindeUitsluitingKiesrecht", 5, 2),
        'dag', substring(kr."DatumEindeUitsluitingKiesrecht", 7, 2)
        )
    END                                                                              AS einddatum_uitsluiting_kiesrecht,
    kr."GemeenteCodeOntlening"::varchar                                              AS gemeente_document,
        CASE -- datum ontlening
      WHEN kr."DatumOntlening" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(kr."DatumOntlening", 1, 4),
            substring(kr."DatumOntlening", 5, 2),
            substring(kr."DatumOntlening", 7, 2)
          ),
        'jaar', substring(kr."DatumOntlening", 1, 4),
        'maand', substring(kr."DatumOntlening", 5, 2),
        'dag', substring(kr."DatumOntlening", 7, 2)
        )
    END                                                                              AS datum_document,
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
