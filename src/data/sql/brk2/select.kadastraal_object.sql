SELECT kot.identificatie                    AS identificatie,
       kot.volgnummer                       AS volgnummer,
       kot.id                               AS id,
       kot.akrkadastralegemeentecode || kot.sectie || LPAD(kot.perceelnummer::text, 5, '0') || kot.index_letter ||
       LPAD(kot.index_nummer::text, 4, '0') AS kadastrale_aanduiding,
       kot.akrkadastralegemeentecode || kot.sectie || LPAD(kot.perceelnummer::text, 5, '0') ||
       kot.index_letter                     AS __kadastrale_aanduiding_minus_index_nummer,
       LPAD(brg."CBSCode"::text, 4, '0')    AS aangeduid_door_brk_gemeente_code,
       brg."BGMNaam"                        AS aangeduid_door_brk_gemeente_omschrijving,
       kge.code                             AS aangeduid_door_brk_kadastralegemeente_code,
       kge.omschrijving                     AS aangeduid_door_brk_kadastralegemeente_omschrijving,
       kot.akrkadastralegemeentecode_code   AS aangeduid_door_brk_kadastralegemeentecode_code,
       kot.akrkadastralegemeentecode        AS aangeduid_door_brk_kadastralegemeentecode_omschrijving,
       kot.akrkadastralegemeentecode || kot.sectie AS aangeduid_door_brk_kadastralesectie,
       kot.perceelnummer                    AS perceelnummer,
       kot.index_letter                     AS indexletter,
       kot.index_nummer                     AS indexnummer,
       brg."BGMNaam"                        AS _gemeente,
       kot.soortgrootte_code                AS soort_grootte_code,
       sge.omschrijving                     AS soort_grootte_omschrijving,
       kot.kadgrootte                       AS grootte,
       kot.cultuurcodeonbebouwd_code        AS soort_cultuur_onbebouwd_code,
       cod.omschrijving                     AS soort_cultuur_onbebouwd_omschrijving,
       kot.cultuurcodebebouwd_code          AS soort_cultuur_bebouwd_code,
       ccb.omschrijving                     AS soort_cultuur_bebouwd_omschrijving,
       kot.status_code                      AS __status,
       kot.referentie                       AS referentie,
       kot.oudst_digitaal_bekend            AS oudst_digitaal_bekend,
       kot.mutatie_id                       AS mutatie_id,
       kot.meettarief_verschuldigd          AS meettarief_verschuldigd,
       kot.toelichting_bewaarder            AS toelichting_bewaarder,
       kot.tijdstip_ontstaan_object         AS tijdstip_ontstaan_object,
       kot.hoofdsplitsing_identificatie     AS hoofdsplitsing_identificatie,
       kot.afwijking_lijst_rechthebbenden   AS afwijking_lijst_rechthebbenden,
       CASE
           WHEN kot.soortgrootte_code IN ('2', '5', '6', '7', '8', '9', '10', '11', '12') THEN 'J'
           ELSE 'N'
           END                              AS indicatie_voorlopige_kadastrale_grens,
       kot.geometrie                        AS __geometrie, -- Temporary field. Only contains G-percelen. A 'geometrie' field will be added later in the finalise step.
       prc.geometrie                        AS plaatscoordinaten,
       prc.rotatie                          AS perceelnummer_rotatie,
       prc.verschuiving_x                   AS perceelnummer_verschuiving_x,
       prc.verschuiving_y                   AS perceelnummer_verschuiving_y,
       bij.geometrie                        AS bijpijling_geometrie,
       kot.koop_som                         AS koopsom,
       kot.koop_valuta_code                 AS koopsom_valutacode,
       kot.koopjaar                         AS koopjaar,
       kot.indicatiemeerobjecten            AS indicatie_meer_objecten,
       kot.toestandsdatum                   AS toestandsdatum,
       ghd.begin_geldigheid                 AS begin_geldigheid,
       ghd.eind_geldigheid                  AS eind_geldigheid,
       ghd.eind_geldigheid                  AS datum_actueel_tot,
       ghd.eind_geldigheid                  AS _expiration_date,
       kok.omschrijving                     AS in_onderzoek,
       adr.vot_adressen                     AS heeft_een_relatie_met_bag_verblijfsobject
FROM brk2.kadastraal_object kot
         JOIN brk2_prep.kot_geldigheid ghd ON ghd.id = kot.id AND ghd.volgnummer = kot.volgnummer
         LEFT JOIN brk2.kadastraal_object_percnummer prc
                   ON kot.id = prc.kadastraalobject_id AND kot.volgnummer = prc.kadastraalobject_volgnummer
         LEFT JOIN brk2.kadastraal_object_bijpijling bij
                   ON kot.id = bij.kadastraalobject_id AND kot.volgnummer = bij.kadastraalobject_volgnummer
         LEFT JOIN brk2.c_kadastralegemeente kge
                   ON kot.kadastralegemeente_code = kge.code
         LEFT JOIN (SELECT "CBSCode", "BGMNaam", "KadGemNaam"
                    FROM brk2.import_burgerlijke_gemeentes
                    GROUP BY "CBSCode", "BGMNaam", "KadGemNaam") brg
                   ON kge.omschrijving = brg."KadGemNaam"
         LEFT JOIN brk2.c_soortgrootte sge
                   ON kot.soortgrootte_code = sge.code
         LEFT JOIN brk2.c_cultuurcodeonbebouwd cod
                   ON kot.cultuurcodeonbebouwd_code = cod.code
         LEFT JOIN brk2.c_cultuurcodebebouwd ccb
                   ON kot.cultuurcodebebouwd_code = ccb.code
         LEFT JOIN brk2.baghulptabel adr
                   ON adr.kadastraalobject_id = kot.id AND adr.kadastraalobject_volgnummer = kot.volgnummer
         LEFT JOIN brk2.kadastraal_object_onderzoek koo
                   ON kot.id = koo.kadastraalobject_id AND kot.volgnummer = koo.kadastraalobject_volgnummer
         LEFT JOIN brk2.inonderzoek io
                   ON koo.onderzoek_identificatie = io.identificatie
         LEFT JOIN brk2.c_authentiekgegeven kok
                   ON io.authentiekgegeven_code = kok.code
;

CREATE INDEX ON brk2_prep.kadastraal_object (id, volgnummer);
CREATE INDEX ON brk2_prep.kadastraal_object (identificatie, volgnummer);
CREATE INDEX ON brk2_prep.kadastraal_object (indexletter);
CREATE INDEX ON brk2_prep.kadastraal_object ((heeft_een_relatie_met_bag_verblijfsobject ->> 'bag_id'));
CREATE INDEX ON brk2_prep.kadastraal_object (aangeduid_door_brk_kadastralesectie);
CREATE INDEX ON brk2_prep.kadastraal_object (aangeduid_door_brk_kadastralegemeentecode_code);
CREATE INDEX ON brk2_prep.kadastraal_object (__kadastrale_aanduiding_minus_index_nummer);
CREATE INDEX ON brk2_prep.kadastraal_object (_expiration_date);
CREATE INDEX ON brk2_prep.kadastraal_object (id);
CREATE INDEX ON brk2_prep.kadastraal_object (hoofdsplitsing_identificatie);
CREATE INDEX ON brk2_prep.kadastraal_object (id, volgnummer, __kadastrale_aanduiding_minus_index_nummer);
