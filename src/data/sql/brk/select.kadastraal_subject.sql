SELECT
          sjt.identificatie                                       AS Identificatie_subject         --stelselpedia
         ,sjt.id AS nrn_sjt_id
         ,CASE
             WHEN sjt.geslachtsnaam IS NOT NULL THEN
              'NATUURLIJK PERSOON'
             WHEN sjt.statutairenaam IS NOT NULL THEN
              'NIET-NATUURLIJK PERSOON'
             WHEN sjt.kad_geslachtsnaam IS NOT NULL THEN
              'NATUURLIJK PERSOON'
             WHEN sjt.kad_naam IS NOT NULL THEN
              'NIET-NATUURLIJK PERSOON'
          END                                                     AS Type_subject                  --stelselpedia
        ,sjt.beschikkingsbevoegdheid_code                         AS Code_Beschikkingsbevoegdheid  --stelselpedia
        ,bbd.omschrijving                                         AS Oms_Beschikkingsbevoegdheid   --stelselpedia
        ,sjt.bsn                                                  AS Heeft_BSN_voor                --stelselpedia
        , CASE WHEN sjt.voornamen             IS NOT NULL THEN sjt.voornamen         ELSE sjt.kad_voornamen
           END                                                    AS Voornamen                     --stelselpedia
        , CASE WHEN sjt.voorvoegselsgeslsnaam IS NOT NULL THEN sjt.voorvoegselsgeslsnaam ELSE sjt.kad_voorvoegselsgeslsnaam
           END                                                    AS Voorvoegsels                  --stelselpedia
        , CASE WHEN sjt.geslachtsnaam         IS NOT NULL THEN sjt.geslachtsnaam     ELSE sjt.kad_geslachtsnaam
           END                                                    AS Geslachtsnaam                 --stelselpedia
        , CASE WHEN sjt.geslacht_code         IS NOT NULL THEN sjt.geslacht_code     ELSE LEFT(agt.omschrijving, 1)
           END                                                    AS Code_geslacht                  --stelselpedia
        , CASE WHEN sjt.geslacht_code         IS NOT NULL THEN ggt.omschrijving      ELSE agt.omschrijving
           END                                                    AS Omschrijving_geslacht          --stelselpedia
        ,sjt.aanduidingnaamgebruik_code                           AS Code_naam_gebruik              --stelselpedia
        ,gnk.omschrijving                                         AS Omschrijving_naam_gebruik      --stelselpedia
        , CASE WHEN sjt.geboortedatum         IS NOT NULL THEN sjt.geboortedatum     ELSE sjt.kad_geboortedatum
           END                                                    AS Geboortedatum                  --stelselpedia
        , CASE WHEN sjt.geboorteplaats        IS NOT NULL THEN sjt.geboorteplaats    ELSE sjt.kad_geboorteplaats
           END                                                    AS Geboorteplaats                 --stelselpedia
        , CASE WHEN sjt.geboorteland_code     IS NOT NULL THEN sjt.geboorteland_code ELSE sjt.kad_geboorteland_code
          END                                                     AS Code_geboorteland              --stelselpedia
        , CASE WHEN gld1.omschrijving         IS NOT NULL THEN gld1.omschrijving     ELSE lad.omschrijving
          END                                                     AS Omschrijving_geboorteland      --stelselpedia
        , CASE
            WHEN bon.overlijdensdatum IS NOT NULL THEN bon.overlijdensdatum
            WHEN sjt.datumoverlijden  IS NOT NULL THEN sjt.datumoverlijden
            ELSE sjt.kad_datumoverlijden
           END                                                    AS Datum_overlijden               --stelselpedia
        ,sjt.kad_indicatieoverleden                               AS Indicatieoverleden             --stelselpedia
        ,sjt.partner_voornamen                                    AS Voornamen_partner              --stelselpedia
        ,sjt.partner_voorvoegselsgeslsnaam                        AS Voorvoegsel_partner            --stelselpedia
        ,sjt.partner_geslachtsnaam                                AS Geslachtsnaam_partner          --stelselpedia
        ,sjt.landwaarnaarvertrokken_code                          AS Code_land_waarnaar_vertrokken  --stelselpedia
        ,gld2.omschrijving                                        AS Omsch_land_waarnaar_vertrokken --stelselpedia
        ,sjt.rsin                                                 AS Heeft_RSIN_voor                --stelselpedia
        ,sjt.kvknummer                                            AS Heeft_KvKnummer_voor          --stelselpedia
         , CASE WHEN sjt.rechtsvorm_code      IS NOT NULL THEN sjt.rechtsvorm_code   ELSE sjt.kad_rechtsvorm_code
          END                                                     AS Code_rechtsvorm                --stelselpedia
         , CASE WHEN nrm.omschrijving IS NOT NULL         THEN nrm.omschrijving               ELSE rvm.omschrijving
          END                                                     AS Omschrijving_rechtsvorm       --stelselpedia
         , CASE WHEN sjt.statutairezetel  = 'NILL'        THEN sjt.kad_statutairezetel       --stelselpedia
                WHEN sjt.statutairezetel  IS NOT NULL     THEN sjt.statutairezetel
                ELSE sjt.kad_statutairezetel
           END                                                    AS Statutaire_zetel       --stelselpedia
        , CASE WHEN sjt.statutairenaam        IS NOT NULL THEN sjt.statutairenaam   ELSE sjt.kad_naam
          END                                                     AS Statutaire_naam       --stelselpedia
-- woonadres
--        ,sws.id                                                  AS nrn_sws_id              --is deze nog nodig GOB??
--        ,sws.subject_id                                          AS nrn_sws_sjt_id          --is deze nog nodig GOB??
         ,sws.adresseerbaar_object_id                             AS Adresseerbaarobject
         ,sws.openbareruimtenaam                                  AS Openbareruimtenaam       --stelselpedia
         ,sws.huisnummer                                          AS Huisnummer               --stelselpedia
         ,sws.huisletter                                          AS Huisletter               --stelselpedia
         ,sws.huisnummertoevoeging                                AS Huisnummertoevoeging     --stelselpedia
         ,sws.postcode                                            AS Postcode                --stelselpedia
         ,sws.woonplaatsnaam                                      AS Woonplaatsnaam           --stelselpedia
         ,sws.buitenland_adres                                    AS Buitenland_adres         --stelselpedia
         ,sws.buitenland_woonplaats                               AS Buitenland_woonplaats    --stelselpedia
         ,sws.buitenland_regio                                    AS Buitenland_regio
         ,sws.buitenland_landnaam                                 AS Buitenland_naam
         ,sws.buitenland_land_code                                AS Buitenland_code
         ,gld3.omschrijving                                       AS Omschrijving_buitenland
         ,bsd.brk_bsd_toestandsdatum                              AS toestandsdatum
-- postadres
--         ,sps.id                                                  AS nrn_sps_id                --is deze nog nodig GOB??
--         ,sps.subject_id                                          AS nrn_sps_sjt_id            --is deze nog nodig GOB??
         ,sps.postbusnummer                                       AS Postbusnummer             --stelselpedia
         ,sps.postbus_postcode                                    AS Postbus_postcode          --stelselpedia
         ,sps.postbus_woonplaatsnaam                              AS Postbus_woonplaatsnaam    --stelselpedia
         ,sps.adresseerbaar_object_id                             AS nrn_sps_aot_id
         ,sps.openbareruimtenaam                                  AS Post_openbareruimtenaam   --stelselpedia
         ,sps.huisnummer                                          AS Post_huisnummer           --stelselpedia
         ,sps.huisletter                                          AS Post_huisletter           --stelselpedia
         ,sps.huisnummertoevoeging                                AS Post_huisnummertoevoeging --stelselpedia
         ,sps.postcode                                            AS Post_postcode             --stelselpedia
         ,sps.woonplaatsnaam                                      AS Post_woonplaatsnaam       --stelselpedia
         ,sps.buitenland_adres                                    AS Post_buitenland_adres     --stelselpedia
         ,sps.buitenland_woonplaats                               AS Post_buitenland_woonplaats
         ,sps.buitenland_regio                                    AS Post_buitenland_regio
         ,sps.buitenland_landnaam                                 AS Post_buitenland_naam
         ,sps.buitenland_land_code                                AS Post_buitenland_code
         ,gld4.omschrijving                                       AS Post_buitenland_oms
         ,ede.expiration_date                                     AS expiration_date
         FROM   brk.subject                           sjt
         LEFT   JOIN brk.subject_woonadres            sws   ON (sjt.id = sws.subject_id)
         LEFT   JOIN brk.subject_postadres            sps   ON (sjt.id = sps.subject_id)
         -- CODETABELLEN
         LEFT   JOIN brk.c_beschikkingsbevoegdheid    bbd   ON (sjt.beschikkingsbevoegdheid_code = bbd.code) -- BESCHIKKINGSBEVOEGDHEID_OMSCHRIJVING
         LEFT   JOIN brk.c_gbaaanduidinggeslacht      ggt   ON (sjt.geslacht_code = ggt.code)                -- GESLACHT_OMSCHRIJVING
         LEFT   JOIN brk.c_gbaaanduidingnaamgebruik   gnk   ON (sjt.aanduidingnaamgebruik_code = gnk.code)   -- AANDUIDINGNAAMGEBRUIK_OMSCHRIJVING
         LEFT   JOIN brk.c_gbaland                   gld1   ON (sjt.geboorteland_code = gld1.code)           -- GEBOORTELAND_OMSCHRIJVING
         LEFT   JOIN brk.c_gbaland                   gld2   ON (sjt.landwaarnaarvertrokken_code = gld2.code) -- LANDWAARNAARVERTROKKEN_OMSCHRIJVING
         LEFT   JOIN brk.c_aanduidinggeslacht         agt   ON (sjt.kad_geslacht_code = agt.code)            -- KAD_GESLACHT_OMSCHRIJVING
         LEFT   JOIN brk.c_land                       lad   ON (sjt.kad_geboorteland_code = lad.code)        -- KAD_GEBOORTELAND_OMSCHRIJVING
         LEFT   JOIN brk.c_nhrrechtsvorm              nrm   ON (sjt.rechtsvorm_code = nrm.code)              -- RECHTSVORM_OMSCHRIJVING
         LEFT   JOIN brk.c_rechtsvorm                 rvm   ON (sjt.kad_rechtsvorm_code = rvm.code)          -- KAD_RECHTSVORM_OMSCHRIJVING
         LEFT   JOIN brk.c_gbaland                   gld3   ON (sws.buitenland_land_code = gld3.code)        -- BUITENLAND_LAND_OMSCHRIJVING
         LEFT   JOIN brk.c_gbaland                   gld4   ON (sps.buitenland_land_code = gld4.code)        -- BUITENLAND_LAND_OMSCHRIJVING
         LEFT   JOIN brk_prep.subject_expiration_date ede   ON (sjt.identificatie=ede.subject_id)
         LEFT   JOIN brk_prep.bsn_overleden           bon   ON (sjt.bsn=bon.bsn)
         JOIN   brk.bestand bsd                          ON (1 = 1)