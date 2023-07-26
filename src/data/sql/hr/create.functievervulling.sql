CREATE TABLE hr_prep.functievervulling AS

  SELECT
    fvv.ashid::varchar                                          AS identificatie,
    NULL::boolean                                               AS langstzittende,
    NULL::date                                                  AS datum_aanvang,
    NULL::date                                                  AS datum_einde,
    NULL::varchar                                               AS functie_titel,
    NULL::boolean                                               AS indicatie_statutair,
    NULL::jsonb                                                 AS schorsing,

    CASE -- aansprakelijke
      WHEN fvv.functie IS NULL THEN NULL::jsonb
      ELSE JSONB_BUILD_OBJECT(
        'code', NULL,
        'omschrijving', fvv.functie
      )
    END                                                         AS aansprakelijke,
    NULL::varchar                                               AS handelingsbekwaam,

    CASE -- bevoegdheids_aansprakelijke
      WHEN fvv.soort IS NULL THEN NULL::jsonb
      ELSE JSONB_BUILD_OBJECT(
        'code', NULL,
        'omschrijving', fvv.soort
      )
    END                                                         AS bevoegdheids_aansprakelijke,

    NULL::jsonb                                                 AS bestuursfunctie,
    NULL::jsonb                                                 AS bevoegdheid_bestuurder,
    NULL::varchar                                               AS vertegenwoordiger_bestuurder_rechtspersoon,
    NULL::jsonb                                                 AS gemachtigde,
    NULL::boolean                                               AS volmacht,
    NULL::boolean                                               AS statutair,
    NULL::jsonb                                                 AS heeft_hr_vestiging, -- relaties VES
    NULL::boolean                                               AS beperkte_volmacht,
    NULL::boolean                                               AS beperking_in_geld,
    NULL::boolean                                               AS doen_van_opgave_aan_handelsregister,
    NULL::boolean                                               AS overige_volmacht,
    NULL::varchar                                               AS omschrijving_overige_beperkingen,
    NULL::boolean                                               AS beperking_in_handeling,
    NULL::varchar                                               AS soort_handeling,
    NULL::boolean                                               AS volledige_volmacht,
    NULL::jsonb                                                 AS overige_functionaris,
    NULL::boolean                                               AS afwijkend_aansprakelijkheidsbeding,
    NULL::jsonb                                                 AS bevoegdheid_funtionaris_volgens_buitlands_recht,
    NULL::jsonb                                                 AS publiekrechtelijke_functionaris,
    NULL::boolean                                               AS bevoegdheid_publiek_rechtelijke_functionaris,
    NULL::varchar                                               AS soort_bevoegdheid,
    NULL::jsonb                                                 AS functionaris_bijzondere_rechtstoestand

  FROM hr.kvkprsashm00 fvv
