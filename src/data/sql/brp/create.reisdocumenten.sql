CREATE TABLE brp_prep.reisdocument AS

  SELECT
    rd."BSN"::varchar                                                                AS burgerservicenummer,
    rd."Anummer"::varchar                                                            AS anummer,
    JSONB_BUILD_OBJECT( -- nationaliteit
      'code', NULL::varchar,
      'omschrijving', rd."SoortNedReisdocument"::varchar
      )                                                                              AS soort_nl_reisdocument,
    rd."NummerNedReisdocument"                                                       AS nummer_nl_reisdocument,
    CASE -- datum eind geldigheid reisdocument
        WHEN rd."DatumUitgifte" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(rd."DatumUitgifte", 1, 4),
            substring(rd."DatumUitgifte", 5, 2),
            substring(rd."DatumUitgifte", 7, 2)
          ),
        'jaar', substring(rd."DatumUitgifte", 1, 4),
        'maand', substring(rd."DatumUitgifte", 5, 2),
        'dag', substring(rd."DatumUitgifte", 7, 2)
        )
    END                                                                              AS datum_verstrekking_nl_reisdocument,
    rd."AutoriteitAfgifte"                                                           AS autoriteit_nl_reisdocument,
    CASE -- datum eind geldigheid reisdocument
      WHEN rd."DatumEindeGeldigheid" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(rd."DatumEindeGeldigheid", 1, 4),
            substring(rd."DatumEindeGeldigheid", 5, 2),
            substring(rd."DatumEindeGeldigheid", 7, 2)
          ),
        'jaar', substring(rd."DatumEindeGeldigheid", 1, 4),
        'maand', substring(rd."DatumEindeGeldigheid", 5, 2),
        'dag', substring(rd."DatumEindeGeldigheid", 7, 2)
        )
    END                                                                            AS datum_einde_geldigheid_nl_reisdocument,

   CASE -- datum inhouding Nl reisdocument
      WHEN rd."DatumInhoudingVermissing" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(rd."DatumInhoudingVermissing", 1, 4),
            substring(rd."DatumInhoudingVermissing", 5, 2),
            substring(rd."DatumInhoudingVermissing", 7, 2)
          ),
        'jaar', substring(rd."DatumInhoudingVermissing", 1, 4),
        'maand', substring(rd."DatumInhoudingVermissing", 5, 2),
        'dag', substring(rd."DatumInhoudingVermissing", 7, 2)
        )
    END                                                                              AS datum_inhouding_Nl_reisdocument,

    NULL::varchar                                                                    AS aanduiding_inhouding_nl_reisdocument,
    NULL::varchar                                                                    AS lengteHouder,
    rd."SignaleringNedReisdocument"                                                  AS signalering_nl_reisdocument,
    rd."GemeenteOntleningCode"                                                       AS gemeente_document,
    NULL                                                                             AS datum_document,
    rd."BeschrijvingDocument"                                                        AS beschrijving_document,

    NULL::varchar                                                                    AS aanduiding_gegevens_in_onderzoek,
    CASE -- datum ingang onderzoek
      WHEN rd."DatumIngangOnderzoek" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(rd."DatumIngangOnderzoek", 1, 4),
          substring(rd."DatumIngangOnderzoek", 5, 2),
          substring(rd."DatumIngangOnderzoek", 7, 2)
        ),
        'jaar', substring(rd."DatumIngangOnderzoek", 1, 4),
        'maand', substring(rd."DatumIngangOnderzoek", 5, 2),
        'dag', substring(rd."DatumIngangOnderzoek", 7, 2)
      )
    END                                                                              AS datum_ingang_onderzoek,

    CASE -- datum einde onderzoek
      WHEN rd."DatumEindeOnderzoek" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
          '-',
          substring(rd."DatumEindeOnderzoek", 1, 4),
          substring(rd."DatumEindeOnderzoek", 5, 2),
          substring(rd."DatumEindeOnderzoek", 7, 2)
        ),
        'jaar', substring(rd."DatumEindeOnderzoek", 1, 4),
        'maand', substring(rd."DatumEindeOnderzoek", 5, 2),
        'dag', substring(rd."DatumEindeOnderzoek", 7, 2)
      )
    END                                                                              AS datum_einde_onderzoek,
    NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

    CASE -- datum opneming
      WHEN rd."DatumOpname" IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'datum', CONCAT_WS(
            '-',
            substring(rd."DatumOpname", 1, 4),
            substring(rd."DatumOpname", 5, 2),
            substring(rd."DatumOpname", 7, 2)
          ),
        'jaar', substring(rd."DatumOpname", 1, 4),
        'maand', substring(rd."DatumOpname", 5, 2),
        'dag', substring(rd."DatumOpname", 7, 2)
        )
    END                                                                              AS datum_opneming,
    NULL::varchar::date                                                              AS datum_actueel_tot -- still have to decide what will be

  FROM brp.reisdocumenten rd
