CREATE TABLE hr_prep.nietnatuurlijkpersoon AS

  SELECT
    nnp.prsid::text                                                     AS _id,
    nnp.rsin::varchar                                                   AS rsin,
    NULL::date                                                          AS datum_actueel_tot,
    datumuitschrijving::text::date                                      AS datum_uitschrijving,
    NULL::text                                                          AS schuldsanering,
    NULL::boolean                                                       AS surceance_van_betaling,
    nnp.status::varchar                                                 AS status,
    SPLIT_PART(nnp.duur, ' ', 1)::integer                               AS duur, -- example of src value: "6 maanden"
    nnp.faillissement::varchar                                          AS faillisement,
    nnp.naam::text                                                      AS naam,
    nnp.volledigenaam                                                   AS volledige_naam,
    nnp.ookgenoemd                                                      AS ook_genoemd,
    nnp.verkortenaam                                                    AS verkorte_naam,
    nnp.typering::varchar                                               AS type_persoon,
    nnp.toegangscode                                                    AS toegangscode,
    nnp.nummer::integer                                                 AS nummer,
    nnp.doelrechtsvorm                                                  AS doelrechtsvorm,
    nnp.rechtsvorm                                                      AS rechtsvorm,
    nnp.persoonsrechtsvorm                                              AS persoon_rechtsvorm,
    nnp.uitgebreiderechtsvorm                                           AS uitgebreide_rechtsvorm,
    nnp.rol::varchar                                                    AS rol,
    NULL::date                                                          AS datum_aanvang,
    NULL::date                                                          AS datum_einde,
    hfvv.ashid                                                          AS heeft_functie_vervulling,
    ifvv.ashid                                                          AS is_functie_vervulling

  FROM
    hr.kvkprsm00 nnp
    LEFT JOIN hr.kvkprsashm00 hfvv ON nnp.prsid = hfvv.prsidh
    LEFT JOIN hr.kvkprsashm00 ifvv ON nnp.prsid = ifvv.prsidi

  WHERE
    nnp.rsin IS NOT NULL
