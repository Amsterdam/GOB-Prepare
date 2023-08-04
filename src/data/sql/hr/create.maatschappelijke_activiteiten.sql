CREATE TABLE hr_prep.maatschappelijke_activiteiten AS

  SELECT
    mac.macid::varchar                                          AS _id,
    mac.kvknummer                                               AS kvknummer,
    NULL::date                                                  AS datum_actueel_tot,
    mac.datumaanvang::text::date                                AS datum_aanvang_maatschappelijke_activiteit,
    mac.datumeinde::text::date                                  AS datum_einde_maatschappelijke_activiteit,
    mac.laatstbijgewerkt                                        AS registratie_tijdstip_maatschappelijke_activiteit,
    mac.naam                                                    AS naam,
    mac.nonmailing                                              AS non_mailing,
    NULL::varchar                                               AS incidenteel_uitlenen_arbeidskrachten,
    NULL::jsonb                                                 AS activiteiten,
    ves.hoofdvestiging_nummer                                   AS heeft_hoofdvestiging,
    ves.datumaanvang::text::date                                AS datum_aanvang_maatschappelijke_activiteit_vestiging,
    ves.datumeinde::text::date                                  AS datum_einde_maatschappelijke_activiteit_vestiging,
    tves.wordt_uitgeoefend_in_ncv                               AS wordt_uitgeoefend_in_niet_commerciele_vestiging,
    CASE -- heeft_als_eigenaar_np; prsid bij gebrek aan BSN (nps.bsn)
      WHEN nps.typering = 'natuurlijkPersoon' THEN nps.prsid::varchar
      ELSE NULL
    END                                                         AS heeft_als_eigenaar_np,
    nps.rsin                                                    AS heeft_als_eigenaar_nnp,
    mac.indicatieonderneming                                    AS onderneming,
    mac.totaalwerkzamepersonen::integer                         AS totaal_werkzame_personen,
    mac.fulltimewerkzamepersonen::integer                       AS voltijd_werkzame_personen,
    mac.parttimewerkzamepersonen::integer                       AS deeltijd_werkzame_personen,
    NULL::date                                                  AS datum_aanvang_onderneming,
    NULL::date                                                  AS datum_einde_onderneming,
    NULL                                                        AS is_overdracht_voortzetting_onderneming,
    NULL::date                                                  AS datum_overdracht_voortzetting_onderneming,
    tves.wordt_uitgeoefend_in_cvs                               AS wordt_uitgeoefend_in_commerciele_vestiging,
    NULL::date                                                  AS datum_aanvang_onderneming_vestiging,
    NULL::date                                                  AS datum_einde_onderneming_vestiging,
    NULL::date                                                  AS datum_aanvang_onderneming_handelsnaam,
    NULL::date                                                  AS datum_einde_onderneming_handelsnaam,
    hn.handelsnamen                                             AS handelsnamen,

    CASE -- communicatie
      WHEN mac.nummer1 IS NULL THEN NULL
      WHEN mac.nummer2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'toegangscode', mac.toegangscode1,
          'nummer', mac.nummer1,
          'soort', mac.soort1
        )
      )
      WHEN mac.nummer3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'toegangscode', mac.toegangscode1,
          'nummer', mac.nummer1,
          'soort', mac.soort1
        ),
        JSONB_BUILD_OBJECT(
          'toegangscode', mac.toegangscode2,
          'nummer', mac.nummer2,
          'soort', mac.soort2
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'toegangscode', mac.toegangscode1,
          'nummer', mac.nummer1,
          'soort', mac.soort1
        ),
        JSONB_BUILD_OBJECT(
          'toegangscode', mac.toegangscode2,
          'nummer', mac.nummer2,
          'soort', mac.soort2
        ),
        JSONB_BUILD_OBJECT(
          'toegangscode', mac.toegangscode3,
          'nummer', mac.nummer3,
          'soort', mac.soort3
        )
      )
    END                                                         AS communicatie,

    CASE -- email_adressen
      WHEN mac.emailadres1 IS NULL THEN NULL
      WHEN mac.emailadres2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'email_adres', mac.emailadres1
        )
      )
      WHEN mac.emailadres3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'email_adres', mac.emailadres1
        ),
        JSONB_BUILD_OBJECT(
          'email_adres', mac.emailadres2
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'email_adres', mac.emailadres1
        ),
        JSONB_BUILD_OBJECT(
          'email_adres', mac.emailadres2
        ),
        JSONB_BUILD_OBJECT(
          'email_adres', mac.emailadres3
        )
      )
    END                                                         AS email_adressen,

    CASE -- domeinnamen
      WHEN mac.domeinnaam1 IS NULL THEN NULL
      WHEN mac.domeinnaam2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'domeinnaam', mac.domeinnaam1
        )
      )
      WHEN mac.domeinnaam3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'domeinnaam', mac.domeinnaam1
        ),
        JSONB_BUILD_OBJECT(
          'domeinnaam', mac.domeinnaam2
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'domeinnaam', mac.domeinnaam1
        ),
        JSONB_BUILD_OBJECT(
          'domeinnaam', mac.domeinnaam2
        ),
        JSONB_BUILD_OBJECT(
          'domeinnaam', mac.domeinnaam3
        )
      )
    END                                                         AS domeinnamen,

    CASE -- bezoek_locatie adres gegevens (indien volledig_adres aanwezig)
      WHEN bezk.volledigadres IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'afgeschermd', bezk.afgeschermd,
        'toevoeging_adres', bezk.toevoegingadres,
        'volledig_adres', bezk.volledigadres,
        'straatnaam', bezk.straatnaam,
        'huisnummer', bezk.huisnummer,
        'huisletter', bezk.huisletter,
        'huisnummer_toevoeging', bezk.huisnummertoevoeging,
        'postcode', bezk.postcode,
        'plaats', bezk.plaats,
        'straat_huisnummer_buitenland', bezk.straathuisnummer,
        'postcode_plaats_buitenland', bezk.postcodewoonplaats,
        'regio_buitenland', bezk.regio,
        'land_buitenland', bezk.land
      )
    END                                                                 AS bezoek_locatie,
    bezk.geopunt                                                        AS bezoek_geopunt,

    -- BAG relaties; zie bag_nummeraanduidingen, bag_verblijfsobjecten, bag_ligplaatsen, bag_standplaatsen
    CASE
      WHEN substring(bezk.identificatieaoa from 5 for 2) = '20' THEN bezk.identificatieaoa
      ELSE NULL
    END                                                                 AS heeft_nummeraanduiding,
    CASE
      WHEN substring(bezk.identificatietgo from 5 for 2) = '01' THEN bezk.identificatietgo
      ELSE NULL
    END                                                                 AS heeft_verblijfsobject,
    CASE
      WHEN substring(bezk.identificatietgo from 5 for 2) = '02' THEN bezk.identificatietgo
      ELSE NULL
    END                                                                 AS heeft_ligplaats,
    CASE
      WHEN substring(bezk.identificatietgo from 5 for 2) = '03' THEN bezk.identificatietgo
      ELSE NULL
    END                                                                 AS heeft_standplaats,

    CASE -- post_locatie adres gegevens (indien volledig_adres aanwezig)
      WHEN post.volledigadres IS NULL THEN NULL
      ELSE JSONB_BUILD_OBJECT(
        'afgeschermd', post.afgeschermd,
        'toevoeging_adres', post.toevoegingadres,
        'volledig_adres', post.volledigadres,
        'straatnaam', post.straatnaam,
        'huisnummer', post.huisnummer,
        'huisletter', post.huisletter,
        'huisnummer_toevoeging', post.huisnummertoevoeging,
        'postbusnummer', post.postbusnummer,
        'postcode', post.postcode,
        'plaats', post.plaats,
        'straat_huisnummer_buitenland', post.straathuisnummer,
        'postcode_plaats_buitenland', post.postcodewoonplaats,
        'regio_buitenland', post.regio,
        'land_buitenland', post.land
      )
    END                                                                 AS post_locatie,
    post.geopunt                                                        AS post_geopunt

  FROM
    hr.kvkmacm00 mac
    LEFT JOIN hr.kvkadrm00 bezk ON mac.macid = bezk.macid AND bezk.typering = 'bezoekLocatie'
    LEFT JOIN hr.kvkadrm00 post ON mac.macid = post.macid AND post.typering = 'postLocatie'

    LEFT JOIN (
      SELECT
        macid,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'datum_aanvang', NULL,
            'datum_einde', NULL,
            'handelsnaam', handelsnaam,
            'volgorde', NULL
          )
          ORDER BY
            hdnid
        ) AS handelsnamen
      FROM
        hr.kvkhdnm00
      GROUP BY
        macid
    ) AS hn ON mac.macid = hn.macid

    LEFT JOIN (
      (
        SELECT
          macid,
          JSONB_AGG(
            JSONB_BUILD_OBJECT('bronwaarde', vestigingsnummer)
            ORDER BY
              vestigingsnummer
          ) AS wordt_uitgeoefend_in_ncv
        FROM
          hr.kvkvesm00
        WHERE
          typeringvestiging = 'NCV' -- NietCommercieleVestiging
        GROUP BY
          macid
      ) AS niet_commercieel FULL
      JOIN (
        SELECT
          macid,
          JSONB_AGG(
            JSONB_BUILD_OBJECT('bronwaarde', vestigingsnummer)
            ORDER BY
              vestigingsnummer
          ) AS wordt_uitgeoefend_in_cvs
        FROM
          hr.kvkvesm00
        WHERE
          typeringvestiging = 'CVS' -- CommercieleVestiging
        GROUP BY
          macid
      ) AS commercieel USING (macid)
    ) tves ON mac.macid = tves.macid

    LEFT JOIN (
      -- Let op, er zijn meerdere hoofdvestigingen!
      SELECT DISTINCT ON (ves.macid)
        ves.vestigingsnummer AS hoofdvestiging_nummer,
        ves.macid,
        ves.datumaanvang,
        ves.datumeinde
      FROM
        hr.kvkvesm00 ves
      WHERE
        ves.indicatiehoofdvestiging = 'Ja' AND ves.datumeinde IS NULL AND ves.datumaanvang IS NOT NULL
      ORDER BY
        ves.macid, ves.datumaanvang DESC
    ) ves ON mac.macid = ves.macid

    LEFT JOIN hr.kvkprsm00 nps USING (prsid)
