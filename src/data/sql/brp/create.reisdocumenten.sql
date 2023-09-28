CREATE TABLE brp_prep.reisdocument AS

  SELECT
    rd."BSN"::varchar                                                                AS burgerservicenummer,
    rd."Anummer"::varchar                                                            AS anummer,
    JSONB_BUILD_OBJECT( -- nationaliteit
      'code', NULL::varchar,
      'omschrijving', rd."SoortNedReisdocument"::text
      )                                                                              AS soort_nl_reisdocument,
    rd."NummerNedReisdocument"                                                       AS nummer_nl_reisdocument,
    CASE -- datum eind geldigheid reisdocument
        WHEN rd."DatumUitgifte" IS NULL THEN NULL
        WHEN rd."DatumUitgifte" = '0' THEN '0000-00-00'
        WHEN length(rd."DatumUitgifte") = 8
          AND rd."DatumUitgifte" != '00000000' THEN CONCAT(
            substring(rd."DatumUitgifte", 1, 4),
            '-',
            substring(rd."DatumUitgifte", 5, 2),
            '-',
            substring(rd."DatumUitgifte", 7, 2)
          )
        ELSE rd."DatumUitgifte"
    END                                                                              AS datum_verstrekking_nl_reisdocument,
    rd."AutoriteitAfgifte"                                                           AS autoriteit_nl_reisdocument,
    CASE -- datum eind geldigheid reisdocument
        WHEN rd."DatumEindeGeldigheid" IS NULL THEN NULL
        WHEN rd."DatumEindeGeldigheid" = '0' THEN '0000-00-00'
        WHEN length(rd."DatumEindeGeldigheid") = 8
          AND rd."DatumEindeGeldigheid" != '00000000' THEN CONCAT(
            substring(rd."DatumEindeGeldigheid", 1, 4),
            '-',
            substring(rd."DatumEindeGeldigheid", 5, 2),
            '-',
            substring(rd."DatumEindeGeldigheid", 7, 2)
          )
        ELSE rd."DatumEindeGeldigheid"
    END                                                                            AS datum_einde_geldigheid_nl_reisdocument,
    CASE -- datum inhouding Nl reisdocument
      WHEN rd."DatumInhoudingVermissing" IS NULL THEN NULL
      WHEN rd."DatumInhoudingVermissing" = '0' THEN '0000-00-00'
      WHEN length(rd."DatumInhoudingVermissing") = 8
        AND rd."DatumInhoudingVermissing" != '00000000' THEN CONCAT(
          substring(rd."DatumInhoudingVermissing", 1, 4),
          '-',
          substring(rd."DatumInhoudingVermissing", 5, 2),
          '-',
          substring(rd."DatumInhoudingVermissing", 7, 2)
        )
      ELSE rd."DatumInhoudingVermissing"
    END                                                                              AS datum_inhouding_Nl_reisdocument,
    NULL::varchar                                                                    AS aanduiding_inhouding_nl_reisdocument,
    NULL::varchar                                                                    AS lengteHouder,
    rd."SignaleringNedReisdocument"                                                  AS signalering_nl_reisdocument,
    rd."GemeenteOntleningCode"                                                       AS gemeente_document,
    NULL                                                                             AS datum_document,
    rd."BeschrijvingDocument"                                                        AS beschrijving_document,
        JSONB_BUILD_OBJECT( -- onderzoek 
      'aanduiding_gegevens_in_onderzoek', NULL,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek
          WHEN rd."DatumIngangOnderzoek" IS NULL THEN NULL
          WHEN rd."DatumIngangOnderzoek" = '0' THEN '0000-00-00'
          WHEN length(rd."DatumIngangOnderzoek") = 8
            AND rd."DatumIngangOnderzoek" != '00000000' THEN CONCAT(
            substring(rd."DatumIngangOnderzoek", 1, 4),
              '-',
              substring(rd."DatumIngangOnderzoek", 5, 2),
              '-',
              substring(rd."DatumIngangOnderzoek", 7, 2)
            )
          ELSE rd."DatumIngangOnderzoek"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek
          WHEN rd."DatumEindeOnderzoek" IS NULL THEN NULL
          WHEN rd."DatumEindeOnderzoek" = '0' THEN '0000-00-00'
          WHEN length(rd."DatumEindeOnderzoek") = 8
            AND rd."DatumEindeOnderzoek" != '00000000' THEN CONCAT(
             substring(rd."DatumEindeOnderzoek", 1, 4),
              '-',
              substring(rd."DatumEindeOnderzoek", 5, 2),
              '-',
              substring(rd."DatumEindeOnderzoek", 7, 2)
            )
          ELSE rd."DatumEindeOnderzoek"::varchar
        END,
       'onjuist_strijdig_openbare_orde', NULL
    )                                                                                AS onderzoek,
    CASE -- datum opneming
      WHEN rd."DatumOpname" IS NULL THEN NULL
      WHEN rd."DatumOpname" = '0' THEN '0000-00-00'
      WHEN length(rd."DatumOpname") = 8
        AND rd."DatumOpname" != '00000000' THEN CONCAT(
          substring(rd."DatumOpname", 1, 4),
          '-',
          substring(rd."DatumOpname", 5, 2),
          '-',
          substring(rd."DatumOpname", 7, 2)
        )
      ELSE rd."DatumOpname"
    END                                                                              AS datum_opneming,
    NULL::text::date                                                                 AS datum_actueel_tot -- still have to decide what will be

  FROM brp.reisdocumenten rd