CREATE TABLE brp_prep.gezagsverhoudingen AS

  SELECT
    gv."BSN"::varchar                                                                AS burgerservicenummer,
    gv."Anummer"::varchar                                                            AS anummer,
    gv."IndGezagMinderjarigeOms"::varchar                                            AS indicatie_gezag_minderjarige,
    gv."IndCurateleRegisterOms"                                                      AS indicatie_curatele_register, -- TODO: Check this
    gv."GemeenteOntleningCode"::varchar                                              AS gemeente_gezagsverhouding_document,
    CASE -- datum ontlening gezagsverhouding
      WHEN gv."DatumOntlening" IS NULL THEN NULL
      WHEN gv."DatumOntlening" = '0'
        OR gv."DatumOntlening" = '00000000' THEN '0000-00-00'
      WHEN length(gv."DatumOntlening") = 8
        AND gv."DatumOntlening" != '00000000' THEN CONCAT_WS(
          '-',
          substring(gv."DatumOntlening", 1, 4),
          substring(gv."DatumOntlening", 5, 2),
          substring(gv."DatumOntlening", 7, 2)
        )
      ELSE gv."DatumOntlening"
    END                                                                              AS datum_ontlening_gezagsverhouding,
    gv."BeschrijvingDocument"                                                        AS beschrijving_document_gezagsverouding,
    JSONB_BUILD_OBJECT( -- onderzoek 
      'aanduiding_gegevens_in_onderzoek', gv."GegevensInOnderzoek"::varchar,
      'datum_ingang_onderzoek', 
        CASE -- datum ingang onderzoek
          WHEN gv."DatumIngangOnderzoek" IS NULL THEN NULL
          WHEN gv."DatumIngangOnderzoek" = '0'
            OR gv."DatumIngangOnderzoek" = '00000000' THEN '0000-00-00'
          WHEN length(gv."DatumIngangOnderzoek") = 8
            AND gv."DatumIngangOnderzoek" != '00000000' THEN CONCAT_WS(
              '-',
              substring(gv."DatumIngangOnderzoek", 1, 4),
              substring(gv."DatumIngangOnderzoek", 5, 2),
              substring(gv."DatumIngangOnderzoek", 7, 2)
            )
          ELSE gv."DatumIngangOnderzoek"::varchar
        END,
      'datum_einde_onderzoek',
        CASE -- datum einde onderzoek
          WHEN gv."DatumEindeOnderzoek" IS NULL THEN NULL
          WHEN gv."DatumEindeOnderzoek" = '0'
            OR gv."DatumEindeOnderzoek" = '00000000' THEN '0000-00-00'
          WHEN length(gv."DatumEindeOnderzoek") = 8
            AND gv."DatumEindeOnderzoek" != '00000000' THEN CONCAT_WS(
              '-',
              substring(gv."DatumEindeOnderzoek", 1, 4),
              substring(gv."DatumEindeOnderzoek", 5, 2),
              substring(gv."DatumEindeOnderzoek", 7, 2)
            )
          ELSE gv."DatumEindeOnderzoek"::varchar
        END,
      'onjuist_strijdig_openbare_orde', NULL::varchar
    )                                                                                AS onderzoek,
    CASE -- ingang datum gelidgheid
      WHEN gv."DatumGeldigheid" IS NULL THEN NULL
      WHEN gv."DatumGeldigheid" = '0'
        OR gv."DatumGeldigheid" = '00000000' THEN '0000-00-00'
      WHEN length(gv."DatumGeldigheid") = 8
        AND gv."DatumGeldigheid" != '00000000' THEN CONCAT_WS(
          '-',
          substring(gv."DatumGeldigheid", 1, 4),
          substring(gv."DatumGeldigheid", 5, 2),
          substring(gv."DatumGeldigheid", 7, 2)
        )
      ELSE gv."DatumGeldigheid"
    END                                                                              AS ingangsdatum_geldigheid,
    CASE -- datum opneming
      WHEN gv."DatumOpname" IS NULL THEN NULL
      WHEN gv."DatumOpname" = '0'
        OR gv."DatumOpname" = '00000000' THEN '0000-00-00'
      WHEN length(gv."DatumOpname") = 8
        AND gv."DatumOpname" != '00000000' THEN CONCAT_WS(
          '-',
          substring(gv."DatumOpname", 1, 4),
          substring(gv."DatumOpname", 5, 2),
          substring(gv."DatumOpname", 7, 2)
        )
      ELSE gv."DatumOpname"
    END                                                                              AS datum_opneming,
    NULL::varchar::date                                                                 AS datum_actueel_tot -- TODO: still have to decide what will be

  FROM brp.gezagsverhouding gv
