SELECT
  gv."BSN"::varchar                                                                AS burgerservicenummer,
  gv."Anummer"::varchar                                                            AS anummer,
  gv."IndGezagMinderjarigeOms"::varchar                                            AS indicatie_gezag_minderjarige,
  gv."IndCurateleRegisterOms"                                                      AS indicatie_curatele_register, -- TODO: Check this
  gv."GemeenteOntleningCode"::varchar                                              AS gemeente_gezagsverhouding_document,
  CASE -- datum ontlening gezagsverhouding
    WHEN gv."DatumOntlening" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(gv."DatumOntlening", 1, 4),
          substring(gv."DatumOntlening", 5, 2),
          substring(gv."DatumOntlening", 7, 2)
        ),
      'jaar', substring(gv."DatumOntlening", 1, 4),
      'maand', substring(gv."DatumOntlening", 5, 2),
      'dag', substring(gv."DatumOntlening", 7, 2)
      )
  END                                                                              AS datum_ontlening_gezagsverhouding,
  gv."BeschrijvingDocument"                                                        AS beschrijving_document_gezagsverouding,

  gv."GegevensInOnderzoek"::varchar                                                AS aanduiding_gegevens_in_onderzoek,
  CASE -- datum ingang onderzoek
    WHEN gv."DatumIngangOnderzoek" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(gv."DatumIngangOnderzoek", 1, 4),
        substring(gv."DatumIngangOnderzoek", 5, 2),
        substring(gv."DatumIngangOnderzoek", 7, 2)
      ),
      'jaar', substring(gv."DatumIngangOnderzoek", 1, 4),
      'maand', substring(gv."DatumIngangOnderzoek", 5, 2),
      'dag', substring(gv."DatumIngangOnderzoek", 7, 2)
    )
  END                                                                              AS datum_ingang_onderzoek,

  CASE -- datum einde onderzoek
    WHEN gv."DatumEindeOnderzoek" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
        '-',
        substring(gv."DatumEindeOnderzoek", 1, 4),
        substring(gv."DatumEindeOnderzoek", 5, 2),
        substring(gv."DatumEindeOnderzoek", 7, 2)
      ),
      'jaar', substring(gv."DatumEindeOnderzoek", 1, 4),
      'maand', substring(gv."DatumEindeOnderzoek", 5, 2),
      'dag', substring(gv."DatumEindeOnderzoek", 7, 2)
    )
  END                                                                              AS datum_einde_onderzoek,
  NULL::varchar                                                                    AS onjuist_strijdig_openbare_orde,

  CASE -- ingang datum gelidgheid
    WHEN gv."DatumGeldigheid" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(gv."DatumGeldigheid", 1, 4),
          substring(gv."DatumGeldigheid", 5, 2),
          substring(gv."DatumGeldigheid", 7, 2)
        ),
      'jaar', substring(gv."DatumGeldigheid", 1, 4),
      'maand', substring(gv."DatumGeldigheid", 5, 2),
      'dag', substring(gv."DatumGeldigheid", 7, 2)
    )
  END                                                                              AS ingangsdatum_geldigheid,
  CASE -- datum opneming
    WHEN gv."DatumOpname" IS NULL THEN NULL
    ELSE JSONB_BUILD_OBJECT(
      'datum', CONCAT_WS(
          '-',
          substring(gv."DatumOpname", 1, 4),
          substring(gv."DatumOpname", 5, 2),
          substring(gv."DatumOpname", 7, 2)
        ),
      'jaar', substring(gv."DatumOpname", 1, 4),
      'maand', substring(gv."DatumOpname", 5, 2),
      'dag', substring(gv."DatumOpname", 7, 2)
      )
  END                                                                              AS datum_opneming,
  NULL::varchar::date                                                              AS datum_actueel_tot -- TODO: still have to decide what will be

FROM brp.gezagsverhouding gv
