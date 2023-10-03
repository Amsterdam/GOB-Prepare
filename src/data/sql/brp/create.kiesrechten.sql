CREATE TABLE brp_prep.kiesrechten AS

  SELECT
    kr."BSN"::varchar                                                                AS burgerservicenummer,
    kr."Anummer"::varchar                                                            AS anummer,
    kr."AandEuropeesKiesrecht"::varchar                                              AS aand_euro_kiesrecht,
    CASE -- datum eu kiesrecht
      WHEN kr."DatumVerzoekEukiesrecht" IS NULL THEN NULL
      WHEN kr."DatumVerzoekEukiesrecht" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
          'datum', '0000-00-00',
          'jaar', '00',
          'maand', '00',
          'dag', '00'
          )
      WHEN length(kr."DatumVerzoekEukiesrecht") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                              AS datum_euro_kiesrecht,
    CASE -- einddatum eu kiesrecht
      WHEN kr."DatumEindeUitsluitingEuKiesrecht" IS NULL THEN NULL
      WHEN kr."DatumEindeUitsluitingEuKiesrecht" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
          'datum', '0000-00-00',
          'jaar', '00',
          'maand', '00',
          'dag', '00'
          )
      WHEN length(kr."DatumEindeUitsluitingEuKiesrecht") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                              AS einddatum_euro_kiesrecht,
    NULL::varchar                                                                    AS adres_e_u_lidstaat_herkomst,  -- deze worden in toekomst geleverd
    NULL::varchar                                                                    AS plaats_e_u_lidstaat_herkomst, -- deze worden in toekomst geleverd
    NULL::varchar                                                                    AS land_e_u_lidstaat_herkomst,   -- deze worden in toekomst geleverd
    kr."AandUitsluitingKiesrecht"::varchar                                           AS aand_uitgesloten_kiesrecht,
    CASE -- einddatum uitsluiting kiesrecht
      WHEN kr."DatumEindeUitsluitingKiesrecht" IS NULL THEN NULL
      WHEN kr."DatumEindeUitsluitingKiesrecht" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
          'datum', '0000-00-00',
          'jaar', '00',
          'maand', '00',
          'dag', '00'
          )
      WHEN length(kr."DatumEindeUitsluitingKiesrecht") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                              AS einddatum_uitsluiting_kiesrecht,
    kr."GemeenteCodeOntlening"::varchar                                              AS gemeente_document,
    CASE -- einddatum ontlening
      WHEN kr."DatumOntlening" IS NULL THEN NULL
      WHEN kr."DatumOntlening" = '0' THEN JSONB_BUILD_OBJECT( -- TODO: NOT definitif. Watting for answer
          'datum', '0000-00-00',
          'jaar', '00',
          'maand', '00',
          'dag', '00'
          )
      WHEN length(kr."DatumOntlening") = 8 THEN JSONB_BUILD_OBJECT(
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
      ELSE NULL
    END                                                                              AS datum_ontlening,
    kr."BeschrijvingDocument"::varchar                                                  AS beschrijving_document,
    NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.kiesrecht kr
