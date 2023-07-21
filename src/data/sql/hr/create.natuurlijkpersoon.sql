 CREATE TABLE hr_prep.natuurlijkpersoon AS

  SELECT
    nps.prsid::varchar                                                  AS identificatie,
    nps.bsn::varchar                                                    AS bsn_nummer, -- altijd leeg
    NULL::date                                                          AS datum_actueel_tot,
    nps.geslachtsnaam                                                   AS geslachtsnaam,
    nps.voorvoegselgeslachtsnaam                                        AS voorvoegsel_geslachtsnaam,
    nps.json_namen                                                      AS voornamen,
    nps.geslachtsaanduiding                                             AS geslachtsaanduiding,
    nps.volledigenaam                                                   AS volledige_naam,
    nps.geboortedatum::text::date                                       AS geboortedatum, -- altijd leeg
    nps.geboorteplaats                                                  AS geboorteplaats,
    nps.geboorteland                                                    AS geboorteland,
    NULL::date                                                          AS overlijdensdatum,
    NULL::varchar                                                       AS schuldsanering,
    NULL::boolean                                                       AS surceance_van_betaling,
    nps.status                                                          AS status,
    split_part(nps.duur, ' ', 1)::integer                               AS duur,
    nps.faillissement                                                   AS faillisement,
    nps.persoonsrechtsvorm                                              AS persoon_rechtsvorm,
    nps.uitgebreiderechtsvorm                                           AS uitgebreide_rechtsvorm,
    nps.typering                                                        AS type_persoon,
    nps.rol                                                             AS rol,
    nps.toegangscode::varchar                                           AS toegangscode,
    nps.nummer::integer                                                 AS nummer,
    fvh.ashid::varchar                                                  AS heeft_functie_vervulling, -- altijd leeg
    fvi.ashid::varchar                                                  AS is_functie_vervulling

  FROM (
    hr.kvkprsm00
    LEFT JOIN (

      SELECT
        prsid,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'voornaam', vor.voornaam
          )
        )                                                               AS json_namen
      FROM (
        SELECT
          prsid,
          unnest(string_to_array(vnn.voornamen, ' ')) AS voornaam
        FROM
          hr.kvkprsm00 vnn
        WHERE vnn.typering = 'natuurlijkPersoon' AND vnn.voornamen IS NOT NULL
      ) vor
      GROUP BY prsid

    ) AS sub USING(prsid)
  ) AS nps

  LEFT JOIN hr.kvkprsashm00 fvh ON nps.prsid = fvh.prsidh
  LEFT JOIN hr.kvkprsashm00 fvi ON nps.prsid = fvi.prsidi
  WHERE typering = 'natuurlijkPersoon'
