SELECT kot.identificatie                    AS brk_kot_id
     , kot.id                               AS nrn_kot_id
     , kot.id                               AS source_id
     , kot.volgnummer                       AS nrn_kot_volgnr
     , json_build_object(
        'code', kot.kadastralegemeente_code,
        'omschrijving', kge.omschrijving
    )                                       AS kad_gemeente
     , json_build_object(
        'code', LPAD(brg.cbscode::text, 4, '0'),
        'omschrijving', brg.bgmnaam
    )                                       AS brg_gemeente
     , json_build_object(
        'code', kot.akrkadastralegemeentecode_code,
        'omschrijving', ake.omschrijving
    )                                       AS kad_gemeentecode
     , ake.omschrijving || kot.sectie || LPAD(kot.perceelnummer::text, 5, '0') || kot.index_letter ||
       LPAD(kot.index_nummer::text, 4, '0') AS kadastrale_aanduiding
     , kot.sectie                           AS sectie
     , kot.perceelnummer                    AS perceelnummer
     , kot.index_letter                     AS index_letter
     , kot.index_nummer                     AS index_nummer
     , kot.soortgrootte_code                AS soortgrootte_code
     , sge.omschrijving                     AS soortgrootte_oms
     , kot.kadgrootte                       AS kadgrootte
     , kot.koop_som                         AS koopsom
     , kot.koop_valuta_code                 AS koopsom_valuta
     , kot.koopjaar                         AS koopjaar
     , kot.indicatiemeerobjecten            AS indicatie_meer_objecten
     , kot.cultuurcodeonbebouwd_code        AS cultuurcodeonbebouwd_code
     , cod.omschrijving                     AS cultuurcodeonbebouwd_oms
     , ccb.cultuurbebouwd                   AS cultuurcodebebouwd
     , kot.status_code                      AS status_code
     , coalesce(vkg.vkgrens, 'N')           AS ind_voorlopige_kadgrens -- Replaced 'Definitieve grens' with 'N'
     , kok.omschrijving                     AS inonderzoek
     , kot.toestandsdatum                   AS toestandsdatum
     , kot.creation                         AS creation
     , kot.modification                     AS modification
     , CASE
           WHEN kot.modification IS NOT NULL
               THEN kot.modification
           ELSE
               (CASE kot.status_code
                    WHEN 'H' THEN kot.creation
                    ELSE NULL END) END      AS expiration_date
     , kot.modification                     as einddatum
     --     Wanneer het een A-perceel betreft
     --     DAN geometrie afleiden uit of meer grondpercelen
     --      in GOB later in het proces--
     , CASE kot.index_letter
           WHEN 'G' THEN
               kot.geometrie
           ELSE
               NULL
    END                                     AS geometrie
     , prc.rotatie                          AS perceelnummer_rotatie
     , prc.verschuiving_x                   AS perceelnummer_verschuiving_x
     , prc.verschuiving_y                   AS perceelnummer_verschuiving_y
     , prc.geometrie                        AS perceelnummer_geometrie
     , bij.geometrie                        AS bijpijling_geometrie
     , adr.adressen                         as adressen
     , brg.bgmnaam                          as brg_gemeente_oms
--
FROM brk.kadastraal_object kot
         LEFT JOIN brk.c_akrkadastralegemeentecode ake
                   ON (akrkadastralegemeentecode_code = ake.code)
         LEFT JOIN brk.c_kadastralegemeente kge
                   ON (kadastralegemeente_code = kge.code)
         LEFT JOIN brk.c_cultuurcodeonbebouwd cod
                   ON (kot.cultuurcodeonbebouwd_code = cod.code)
         LEFT JOIN brk.c_soortgrootte sge
                   ON (kot.soortgrootte_code = sge.code)
         LEFT JOIN brk.kadastraalobject_onderzoek kok
                   ON (kot.id = kok.kadastraalobject_id AND
                       kot.volgnummer = kok.kadastraalobject_volgnummer)
--Cultuurcode bebouwd, kunnen er meer per kadastraal object zijn
         LEFT JOIN (SELECT kas.kot_id                                                               AS nrn_kot_id
                         , kas.kot_volgnr                                                           AS nrn_kot_volgnr
                         , array_to_json(array_agg(json_build_object( -- POSTGRES Changed to JSON
                                                           'code', kas.cult_beb_code,
                                                           'omschrijving', kas.cult_beb
                                                       ) ORDER BY kas.cult_beb_code, kas.cult_beb)) as cultuurbebouwd
                    FROM (SELECT kasi.kadastraalobject_id         AS kot_id
                               , kasi.kadastraalobject_volgnummer AS kot_volgnr
                               , cbd.omschrijving                 AS cult_beb
                               , cbd.code                         AS cult_beb_code
                          FROM brk.kadastraal_adres kasi
                                   JOIN brk.c_cultuurcodebebouwd cbd
                                        ON (kasi.cultuurbebouwd_code = cbd.code)
                          WHERE kasi.cultuurbebouwd_code IS NOT NULL
                          GROUP BY kasi.kadastraalobject_id
                                 , kasi.kadastraalobject_volgnummer
                                 , cbd.omschrijving
                                 , cbd.code) kas
                    GROUP BY kas.kot_id
                           , kas.kot_volgnr) ccb
                   ON (kot.id = ccb.nrn_kot_id AND kot.volgnummer = ccb.nrn_kot_volgnr)
    -- Voorlopige_grens: indicatie voorlopige kadastrale grens wordt afgeleid uit aantekening en aantekening_kadastraalobject
-- 270 Administratieve voorlopige (kadastrale) grens
-- 271 Voorlopige kadastrale grens en oppervlakte
-- Het lijkt erop dat deze beperkingen nooit beeindigd worden door het Kadaster. Dit moet nog nagevraagd worden
         LEFT JOIN (SELECT akt.kadastraalobject_id         AS nrn_kot_id
                         , akt.kadastraalobject_volgnummer AS nrn_kot_volgnr
                         , 'J'                             AS vkgrens -- Replace 'Voorlopige grens' with 'J'
                    FROM brk.aantekening_kadastraalobject akt
                       , brk.aantekening atg
                    WHERE atg.id = akt.aantekening_id
                      AND (date_trunc('day', einddatum) <= date_trunc('day', NOW()) or -- POSTGRES: replaced trunc(einddatum) with date_trunc('day', einddatum) and trunc(SYSDATE) with date_trun('day', NOW())
                           einddatum IS NULL)
                      AND aardaantekening_code IN ('270', '271')
                      --ontdubbel voorlopige grenzen per kadastraal object en cyclus (diva 2.30.8)
                      AND (akt.kadastraalobject_id, kadastraalobject_volgnummer,
                           aantekening_id) IN
                          (SELECT akt.kadastraalobject_id
                                , akt.kadastraalobject_volgnummer
                                , MAX(aantekening_id)
                           FROM brk.aantekening_kadastraalobject akt
                              , brk.aantekening atg
                           WHERE atg.id = akt.aantekening_id
                             AND (date_trunc('day', einddatum) <= date_trunc('day', NOW()) OR -- POSTGRES: replaced trunc(einddatum) with date_trunc('day', einddatum) and trunc(SYSDATE) with date_trun('day', NOW())
                                  einddatum IS NULL)
                             AND aardaantekening_code IN
                                 ('270', '271')
                           GROUP BY akt.kadastraalobject_id
                                  , akt.kadastraalobject_volgnummer)) vkg
                   ON (kot.id = vkg.nrn_kot_id AND kot.volgnummer = vkg.nrn_kot_volgnr)
         LEFT OUTER JOIN brk.perceelnummer prc
                         ON (kot.id = prc.id AND kot.volgnummer = prc.volgnummer)
         LEFT OUTER JOIN brk.bijpijling bij
                         ON (kot.id = bij.id AND kot.volgnummer = bij.volgnummer)
         left outer join brk.baghulptabel adr
                         on adr.kadastraalobject_id = kot.id and adr.kadastraalobject_volgnummer = kot.volgnummer
         left join (select cbscode, bgmnaam, kadgemnaam
                    from brk.import_burgerlijke_gemeentes
                    group by cbscode, bgmnaam, kadgemnaam) brg
                   on (kge.omschrijving = brg.kadgemnaam);