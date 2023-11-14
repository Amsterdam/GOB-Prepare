SET max_parallel_workers_per_gather = 0;
CREATE TABLE brk2_prep.kadastraal_subject USING columnar AS
WITH subjecten AS ((SELECT id                            AS nrn_sjt_id
                         , identificatie                 AS Identificatie_subject
                         , 'NATUURLIJK PERSOON'          AS Type_subject
                         , beschikkingsbevoegdheid_code  AS Code_Beschikkingsbevoegdheid
                         , ind_diakriet_niet_toonbaar    AS Indicatie_diakriet_niet_toonbaar
                         , postlocatie_identificatie     AS Postlocatie_identificatie
                         , postlocatietype               AS Postlocatie_type
                         , woonlocatie_identificatie     AS Woonlocatie_identificatie
                         , woonlocatietype               AS Woonlocatie_type
                         , ind_overleden                 AS Indicatie_overleden
                         , ind_afscherming_gegevens      AS Indicatie_afscherming_gegevens
                         , bsn                           AS Heeft_BSN_voor
                         , titel_of_predicaat_code       AS Code_titel_of_predicaat
                         , aanduiding_naamgebruik_code   AS Code_naam_gebruik
                         , land_waarnaar_vertrokken_code AS Code_land_waarnaar_vertrokken
                         , geslachtsnaam                 AS Geslachtsnaam
                         , voornamen                     AS Voornamen
                         , voorvoegsel                   AS Voorvoegsels
                         , geslachtsaanduiding_code      AS Code_geslacht
                         , geboortedatum                 AS Geboortedatum
                         , geboortedatum_onv             AS Geboortedatum_onvolledig
                         , geboorteplaats                AS Geboorteplaats
                         , geboorteland_code             AS Code_geboorteland
                         , overlijdensdatum              AS Datum_overlijden
                         , overlijdensdatum_onv          AS Datum_overlijden_onvolledig
                         , partner_geslachtsnaam         AS Geslachtsnaam_partner
                         , partner_voornamen             AS Voornamen_partner
                         , partner_voorvoegsel           AS Voorvoegsel_partner
                         , NULL                          AS Statutaire_naam
                         , NULL                          AS Statutaire_zetel
                         , NULL                          AS Code_rechtsvorm
                         , NULL                          AS Heeft_RSIN_voor
                         , NULL                          AS Heeft_KvKnummer_voor
                    FROM brk2.natuurlijk_persoon)
                   UNION
                   (SELECT id                           AS nrn_sjt_id
                         , identificatie                AS Identificatie_subject
                         , 'NIET-NATUURLIJK PERSOON'    AS Type_subject
                         , beschikkingsbevoegdheid_code AS Code_Beschikkingsbevoegdheid
                         , ind_diakriet_niet_toonbaar   AS Indicatie_diakriet_niet_toonbaar
                         , postlocatie_identificatie    AS Postlocatie_identificatie
                         , postlocatietype              AS Postlocatie_type
                         , woonlocatie_identificatie    AS Woonlocatie_identificatie
                         , woonlocatietype              AS Woonlocatie_type
                         , NULL                         AS Indicatie_overleden
                         , NULL                         AS Indicatie_afscherming_gegevens
                         , NULL                         AS Heeft_BSN_voor
                         , NULL                         AS Code_titel_of_predicaat
                         , NULL                         AS Code_naam_gebruik
                         , NULL                         AS Code_land_waarnaar_vertrokken
                         , NULL                         AS Geslachtsnaam
                         , NULL                         AS Voornamen
                         , NULL                         AS Voorvoegsels
                         , NULL                         AS Code_geslacht
                         , NULL                         AS Geboortedatum
                         , NULL                         AS Geboortedatum_onvolledig
                         , NULL                         AS Geboorteplaats
                         , NULL                         AS Code_geboorteland
                         , NULL                         AS Datum_overlijden
                         , NULL                         AS Datum_overlijden_onvolledig
                         , NULL                         AS Geslachtsnaam_partner
                         , NULL                         AS Voornamen_partner
                         , NULL                         AS Voorvoegsel_partner
                         , statutairenaam               AS Statutaire_naam
                         , statutairezetel              AS Statutaire_zetel
                         , rechtsvorm_code              AS Code_rechtsvorm
                         , rsin                         AS Heeft_RSIN_voor
                         , kvknummer                    AS Heeft_KvKnummer_voor
                    FROM brk2.niet_natuurlijk_persoon))
SELECT sjt.Identificatie_subject            AS identificatie,
       sjt.Type_subject                     AS type_subject,
       sjt.Code_Beschikkingsbevoegdheid     AS beschikkingsbevoegdheid_code,
       bbd.omschrijving                     AS beschikkingsbevoegdheid_omschrijving,
       sjt.Indicatie_afscherming_gegevens   AS indicatie_afscherming_gegevens,
       sjt.Heeft_BSN_voor                   AS heeft_bsn_voor_brp_persoon,
       sjt.Voornamen                        AS voornamen,
       sjt.Voorvoegsels                     AS voorvoegsels,
       sjt.Geslachtsnaam                    AS geslachtsnaam,
       sjt.Code_geslacht                    AS geslacht_code,
       aag.omschrijving                     AS geslacht_omschrijving,
       sjt.Code_naam_gebruik                AS naam_gebruik_code,
       ank.omschrijving                     AS naam_gebruik_omschrijving,
       sjt.Code_titel_of_predicaat          AS titel_of_predicaat_code,
       ctp.omschrijving                     AS titel_of_predicaat_omschrijving,
       sjt.Indicatie_diakriet_niet_toonbaar AS indicatie_diakriet_niet_toonbaar,
       sjt.Geboortedatum::date              AS geboortedatum,
       sjt.Geboortedatum_onvolledig         AS geboortedatum_onvolledig,
       sjt.Geboorteplaats                   AS geboorteplaats,
       sjt.Code_geboorteland                AS geboorteland_code,
       lad.omschrijving                     AS geboorteland_omschrijving,
       COALESCE (bon.overlijdensdatum::date,
           sjt.Datum_overlijden::date)      AS datum_overlijden,
       sjt.Datum_overlijden_onvolledig      AS datum_overlijden_onvolledig,
       sjt.Indicatie_overleden              AS indicatie_overleden,
       sjt.Voornamen_partner                AS voornamen_partner,
       sjt.Voorvoegsel_partner              AS voorvoegsels_partner,
       sjt.Geslachtsnaam_partner            AS geslachtsnaam_partner,
       sjt.Heeft_RSIN_voor                  AS heeft_rsin_voor_hr_niet_natuurlijkepersoon,
       sjt.Heeft_KvKnummer_voor             AS heeft_kvknummer_voor_hr_maatschappelijkeactiviteit,
       sjt.Code_rechtsvorm                  AS rechtsvorm_code,
       rvm.omschrijving                     AS rechtsvorm_omschrijving,
       sjt.Statutaire_naam                  AS statutaire_naam,
       sjt.Statutaire_zetel                 AS statutaire_zetel,
       sjt.Woonlocatie_type::integer        AS woonadres_type,
       obd.bag_identificatie                AS woonadres_adresseerbaar_object,
       obd.openbareruimtenaam               AS woonadres_openbare_ruimtenaam,
       obd.huisnummer::integer              AS woonadres_huisnummer,
       obd.huisletter                       AS woonadres_huisletter,
       obd.huisnummertoevoeging             AS woonadres_huisnummertoevoeging,
       obd.postcode                         AS woonadres_postcode,
       obd.woonplaatsnaam                   AS woonadres_woonplaats,
       obd.woonplaatsnaam_afwijkend         AS woonadres_woonplaats_afwijkend,
       obu.adres                            AS woonadres_buitenland_adres,
       obu.woonplaats                       AS woonadres_buitenland_woonplaats,
       obu.regio                            AS woonadres_buitenland_regio,
       obu.land_code                        AS woonadres_buitenland_land_code,
       lbu.omschrijving                     AS woonadres_buitenland_land_omschrijving,
       sjt.Code_land_waarnaar_vertrokken    AS land_waarnaar_vertrokken_code,
       lav.omschrijving                     AS land_waarnaar_vertrokken_omschrijving,
       sjt.Postlocatie_type::integer        AS postadres_type,
       pad.bag_identificatie                AS postadres_adresseerbaar_object,
       pad.openbareruimtenaam               AS postadres_openbare_ruimtenaam,
       pad.huisnummer::integer              AS postadres_huisnummer,
       pad.huisletter                       AS postadres_huisletter,
       pad.huisnummertoevoeging             AS postadres_huisnummertoevoeging,
       pad.postcode                         AS postadres_postcode,
       pad.woonplaatsnaam                   AS postadres_woonplaats,
       pad.woonplaatsnaam_afwijkend         AS postadres_woonplaats_afwijkend,
       pau.adres                            AS postadres_buitenland_adres,
       pau.woonplaats                       AS postadres_buitenland_woonplaats,
       pau.regio                            AS postadres_buitenland_regio,
       pau.land_code                        AS postadres_buitenland_land_code,
       lbu.omschrijving                     AS postadres_buitenland_land_omschrijving,
       pbl.postbusnummer::integer           AS postadres_postbus_nummer,
       pbl.postcode                         AS postadres_postbus_postcode,
       pbl.woonplaatsnaam                   AS postadres_postbus_woonplaatsnaam,
       ede.expiration_date::timestamp       AS datum_actueel_tot,
       ede.expiration_date::timestamp       AS _expiration_date,
       NULL                                 AS toestandsdatum
FROM subjecten sjt
         LEFT JOIN brk2.objectlocatie_binnenland obd ON (sjt.woonlocatie_identificatie = obd.identificatie)
         LEFT JOIN brk2.objectlocatie_buitenland obu ON (sjt.woonlocatie_identificatie = obu.identificatie)
         LEFT JOIN brk2.objectlocatie_binnenland pad ON (sjt.postlocatie_identificatie = pad.identificatie)
         LEFT JOIN brk2.objectlocatie_buitenland pau ON (sjt.postlocatie_identificatie = pau.identificatie)
         LEFT JOIN brk2.postbus_locatie pbl ON (sjt.postlocatie_identificatie = pbl.identificatie)
         LEFT JOIN brk2.c_beschikkingsbevoegdheid bbd ON (sjt.Code_beschikkingsbevoegdheid = bbd.code)
         LEFT JOIN brk2.c_aanduidinggeslacht aag ON (sjt.Code_geslacht = aag.code)
         LEFT JOIN brk2.c_aanduidingnaamgebruik ank ON (sjt.Code_naam_gebruik = ank.code)
         LEFT JOIN brk2.c_aanduidinggeslacht agt ON (sjt.Code_geslacht = agt.code)
         LEFT JOIN brk2.c_land lad ON (sjt.Code_geboorteland = lad.code)
         LEFT JOIN brk2.c_land lav ON (sjt.Code_land_waarnaar_vertrokken = lav.code)
         LEFT JOIN brk2.c_rechtsvorm rvm ON (sjt.Code_rechtsvorm = rvm.code)
         LEFT JOIN brk2.c_titelofpredicaat ctp ON (sjt.Code_titel_of_predicaat = ctp.code)
         LEFT JOIN brk2.c_land lbu ON (obu.land_code = lbu.code)
         LEFT JOIN brk2.c_land pbu ON (pau.land_code = pbu.code)
         LEFT JOIN brk2_prep.subject_expiration_date ede ON sjt.Identificatie_subject = ede.subject_id
         LEFT JOIN brk2.bsn_overleden bon ON (sjt.bsn = bon.bsn)
         JOIN brk2_prep.meta meta ON TRUE
;
