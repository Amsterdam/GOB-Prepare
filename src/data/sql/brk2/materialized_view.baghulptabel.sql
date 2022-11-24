CREATE MATERIALIZED VIEW brk2.baghulptabel AS
SELECT kadastraalobject_id,
       kadastraalobject_volgnummer,
       JSONB_AGG(
       adres ORDER BY
           koppelingswijze_code,
           koppelingswijze_omschrijving,
           bag_id,
           openbareruimtenaam,
           huisnummer,
           huisletter,
           huisnummertoevoeging,
           postcode,
           woonplaatsnaam,
           woonplaatsnaam_afwijkend,
           hoofdadres_identificatie,
           typering
           ) FILTER (WHERE typering = 'VBO') AS vot_adressen,
       JSONB_AGG(
       adres ORDER BY
           koppelingswijze_code,
           koppelingswijze_omschrijving,
           bag_id,
           openbareruimtenaam,
           huisnummer,
           huisletter,
           huisnummertoevoeging,
           postcode,
           woonplaatsnaam,
           woonplaatsnaam_afwijkend,
           hoofdadres_identificatie,
           typering
           ) FILTER (WHERE typering = 'STA') AS sps_adressen,
       JSONB_AGG(
       adres ORDER BY
           koppelingswijze_code,
           koppelingswijze_omschrijving,
           bag_id,
           openbareruimtenaam,
           huisnummer,
           huisletter,
           huisnummertoevoeging,
           postcode,
           woonplaatsnaam,
           woonplaatsnaam_afwijkend,
           hoofdadres_identificatie,
           typering
           ) FILTER (WHERE typering = 'LIG') AS lps_adressen
FROM (SELECT kol.kadastraalobject_id,
             kol.kadastraalobject_volgnummer,
             JSONB_BUILD_OBJECT(
                     'koppelingswijze_code', koppelingswijze_code,
                     'koppelingswijze_omschrijving', ckw.omschrijving,
                     'bag_id', aol.bag_identificatie,
                     'openbareruimtenaam', openbareruimtenaam,
                     'huisnummer', huisnummer,
                     'huisletter', huisletter,
                     'huisnummertoevoeging', huisnummertoevoeging,
                     'postcode', postcode,
                     'woonplaatsnaam', woonplaatsnaam,
                     'woonplaatsnaam_afwijkend', woonplaatsnaam_afwijkend,
                     'hoofdadres_identificatie', hoofdadres_identificatie,
                     'typering', typering
                 )                 AS adres,
             kol.koppelingswijze_code,
             ckw.omschrijving      AS koppelingswijze_omschrijving,
             aol.bag_identificatie AS bag_id,
             obl.openbareruimtenaam,
             obl.huisnummer,
             obl.huisletter,
             obl.huisnummertoevoeging,
             obl.postcode,
             obl.woonplaatsnaam,
             obl.woonplaatsnaam_afwijkend,
             aol.hoofdadres_identificatie,
             aol.typering
      FROM brk2.kadastraal_object_locatie kol
               LEFT JOIN brk2.c_koppelingswijze ckw
                         ON (kol.koppelingswijze_code = ckw.code)
               LEFT JOIN brk2.kadastraal_object kot
                         ON (kol.kadastraalobject_id = kot.id AND kol.kadastraalobject_volgnummer = kot.volgnummer)
               LEFT JOIN brk2.objectlocatie_binnenland obl
                         ON kol.locatie_identificatie = obl.identificatie
               LEFT JOIN brk2.adresseerbaar_object aol
                         ON kol.locatie_identificatie = aol.hoofdadres_identificatie) q
GROUP BY kadastraalobject_id, kadastraalobject_volgnummer
;