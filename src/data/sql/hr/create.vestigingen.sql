CREATE TABLE hr_prep.vestigingen AS

  SELECT
    ves.vesid::text                                                     AS _id,
    ves.vestigingsnummer                                                AS vestigingsnummer,
    NULL::date                                                          AS datum_actueel_tot,
    ves.datumaanvang::text::date                                        AS datum_aanvang,
    ves.datumeinde::text::date                                          AS datum_einde,
    NULL::date                                                          AS datum_voortzetting,
    ves.typeringvestiging                                               AS is_commerciele_vestiging,
    ves.eerstehandelsnaam                                               AS eerste_handelsnaam,

    CASE -- communicatie
      WHEN ves.nummer1 IS NULL THEN NULL
      WHEN ves.nummer2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'toegangscode', ves.toegangscode1,
          'nummer', ves.nummer1,
          'soort', ves.soort1
        )
      )
      WHEN ves.nummer3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'toegangscode', ves.toegangscode1,
          'nummer', ves.nummer1,
          'soort', ves.soort1
        ),
        JSONB_BUILD_OBJECT(
          'toegangscode', ves.toegangscode2,
          'nummer', ves.nummer2,
          'soort', ves.soort2
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'toegangscode', ves.toegangscode1,
          'nummer', ves.nummer1,
          'soort', ves.soort1
        ),
        JSONB_BUILD_OBJECT(
          'toegangscode', ves.toegangscode2,
          'nummer', ves.nummer2,
          'soort', ves.soort2
        ),
        JSONB_BUILD_OBJECT(
          'toegangscode', ves.toegangscode3,
          'nummer', ves.nummer3,
          'soort', ves.soort3
        )
      )
    END                                                                 AS communicatie,

    CASE -- email_adressen
      WHEN ves.emailadres1 IS NULL THEN NULL
      WHEN ves.emailadres2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'email_adres', ves.emailadres1
        )
      )
      WHEN ves.emailadres3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'email_adres', ves.emailadres1
        ),
        JSONB_BUILD_OBJECT(
          'email_adres', ves.emailadres2
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'email_adres', ves.emailadres1
        ),
        JSONB_BUILD_OBJECT(
          'email_adres', ves.emailadres2
        ),
        JSONB_BUILD_OBJECT(
          'email_adres', ves.emailadres3
        )
      )
    END                                                                 AS email_adressen,

    CASE -- domeinnamen
      WHEN ves.domeinnaam1 IS NULL THEN NULL
      WHEN ves.domeinnaam2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'domeinnaam', ves.domeinnaam1
        )
      )
      WHEN ves.domeinnaam3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'domeinnaam', ves.domeinnaam1
        ),
        JSONB_BUILD_OBJECT(
          'domeinnaam', ves.domeinnaam2
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'domeinnaam', ves.domeinnaam1
        ),
        JSONB_BUILD_OBJECT(
          'domeinnaam', ves.domeinnaam2
        ),
        JSONB_BUILD_OBJECT(
          'domeinnaam', ves.domeinnaam3
        )
      )
    END                                                                 AS domeinnamen,

    NULL                                                                AS is_samengevoegd_met_vestigingen,
    NULL::date                                                          AS datum_afgesloten,
    NULL::date                                                          AS datum_samenvoeging,
    ves.naam                                                            AS naam,
    ves.verkortenaam                                                    AS verkorte_naam,
    ves.ookgenoemd                                                      AS ook_genoemd,
    ves.totaalwerkzamepersonen::integer                                 AS totaal_werkzame_personen,
    ves.fulltimewerkzamepersonen::integer                               AS voltijd_werkzame_personen,
    ves.parttimewerkzamepersonen::integer                               AS deeltijd_werkzame_personen,
    ves.indicatiehoofdvestiging                                         AS hoofd_vestiging,
    ves.omschrijvingactiviteit                                          AS activiteit_omschrijving,
    ves.importactiviteit                                                AS importeert,
    ves.exportactiviteit                                                AS exporteert,

    CASE -- activiteiten
      WHEN ves.sbicodehoofdactiviteit IS NULL THEN NULL
      WHEN ves.sbicodenevenactiviteit1 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodehoofdactiviteit,
          'omschrijving', ves.sbiomschrijvinghoofdact,
          'is_hoofdactiviteit', 'Ja',
          'volgorde', 1 -- ?
        )
      )
      WHEN ves.sbicodenevenactiviteit2 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodehoofdactiviteit,
          'omschrijving', ves.sbiomschrijvinghoofdact,
          'is_hoofdactiviteit', 'Ja',
          'volgorde', 1 -- ?
        ),
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodenevenactiviteit1,
          'omschrijving', ves.sbiomschrijvingnevenact1,
          'is_hoofdactiviteit', 'Nee',
          'volgorde', 2 -- ?
        )
      )
      WHEN ves.sbicodenevenactiviteit3 IS NULL THEN JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodehoofdactiviteit,
          'omschrijving', ves.sbiomschrijvinghoofdact,
          'is_hoofdactiviteit', 'Ja',
          'volgorde', 1 -- ?
        ),
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodenevenactiviteit1,
          'omschrijving', ves.sbiomschrijvingnevenact1,
          'is_hoofdactiviteit', 'Nee',
          'volgorde', 2 -- ?
        ),
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodenevenactiviteit2,
          'omschrijving', ves.sbiomschrijvingnevenact2,
          'is_hoofdactiviteit', 'Nee',
          'volgorde', 3 -- ?
        )
      )
      ELSE JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodehoofdactiviteit,
          'omschrijving', ves.sbiomschrijvinghoofdact,
          'is_hoofdactiviteit', 'Ja',
          'volgorde', 1 -- ?
        ),
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodenevenactiviteit1,
          'omschrijving', ves.sbiomschrijvingnevenact1,
          'is_hoofdactiviteit', 'Nee',
          'volgorde', 2 -- ?
        ),
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodenevenactiviteit2,
          'omschrijving', ves.sbiomschrijvingnevenact2,
          'is_hoofdactiviteit', 'Nee',
          'volgorde', 3 -- ?
        ),
        JSONB_BUILD_OBJECT(
          'sbi_code', ves.sbicodenevenactiviteit3,
          'omschrijving', ves.sbiomschrijvingnevenact3,
          'is_hoofdactiviteit', 'Nee',
          'volgorde', 4 -- ?
        )
      )
    END                                                                 AS activiteiten,

    hn.handelsnamen                                                     AS handelsnamen,
    mac.kvknummer                                                       AS is_een_uitoefening_van,

    CASE -- bezoek_locatie adres gegevens (indien volledig_adres aanwezig voor vesid)
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
    END                                                                 AS bezoek_heeft_nummeraanduiding,
    CASE
      WHEN substring(bezk.identificatietgo from 5 for 2) = '01' THEN bezk.identificatietgo
      ELSE NULL
    END                                                                 AS bezoek_heeft_verblijfsobject,
    CASE
      WHEN substring(bezk.identificatietgo from 5 for 2) = '02' THEN bezk.identificatietgo
      ELSE NULL
    END                                                                 AS bezoek_heeft_ligplaats,
    CASE
      WHEN substring(bezk.identificatietgo from 5 for 2) = '03' THEN bezk.identificatietgo
      ELSE NULL
    END                                                                 AS bezoek_heeft_standplaats,

    CASE -- post_locatie adres gegevens (indien volledig_adres aanwezig voor vesid)
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
    post.geopunt                                                        AS post_geopunt,

    -- BAG relaties; zie bag_nummeraanduidingen, bag_verblijfsobjecten, bag_ligplaatsen, bag_standplaatsen
    CASE
      WHEN substring(post.identificatieaoa from 5 for 2) = '20' THEN post.identificatieaoa
      ELSE NULL
    END                                                                 AS post_heeft_nummeraanduiding,
    CASE
      WHEN substring(post.identificatietgo from 5 for 2) = '01' THEN post.identificatietgo
      ELSE NULL
    END                                                                 AS post_heeft_verblijfsobject,
    CASE
      WHEN substring(post.identificatietgo from 5 for 2) = '02' THEN post.identificatietgo
      ELSE NULL
    END                                                                 AS post_heeft_ligplaats,
    CASE
      WHEN substring(post.identificatietgo from 5 for 2) = '03' THEN post.identificatietgo
      ELSE NULL
    END                                                                 AS post_heeft_standplaats

  FROM
    hr.kvkvesm00 ves
    LEFT JOIN hr.kvkmacm00 mac ON ves.macid = mac.macid

    LEFT JOIN (
      SELECT
        vesid,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'datum_aanvang', vhn.beginrelatie,
            'datum_einde', vhn.eindrelatie,
            'handelsnaam', hn.handelsnaam,
            'volgorde', NULL
          )
          ORDER BY
            hdnid
        )                                                               AS handelsnamen
      FROM
        hr.kvkveshdnm00 vhn
        LEFT JOIN hr.kvkhdnm00 hn USING(hdnid)
      GROUP BY
        vesid
    ) AS hn USING(vesid)

    LEFT JOIN hr.kvkadrm00 bezk ON ves.vesid = bezk.vesid AND bezk.typering = 'bezoekLocatie'
    LEFT JOIN hr.kvkadrm00 post ON ves.vesid = post.vesid AND post.typering = 'postLocatie'
