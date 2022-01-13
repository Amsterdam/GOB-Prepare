create index on brk.aantekening(aardaantekening_code);
create index on brk.aantekening(einddatum);
create index on brk.aantekening(id);
create index on brk.aantekening_kadastraalobject(aantekening_id);
create index on brk.aantekening_kadastraalobject(kadastraalobject_id);
create index on brk.aantekening_kadastraalobject(kadastraalobject_volgnummer);
create index on brk.aantekeningbetrokkenpersoon(aantekening_id);
create index on brk.aantekeningisgebaseerdop(aantekening_id);
create index on brk.aantekeningisgebaseerdop(stukdeel_identificatie);
create index on brk.aantekeningrecht(aantekening_id);
create index on brk.aantekeningrecht(tenaamstelling_identificatie);
create index on brk.adresseerbaar_object(id);
create index on brk.appartementsrechtspl_stukdeel(appartementsrechtsplitsing_id);
create index on brk.appartementsrechtspl_stukdeel(stukdeel_identificatie);
create index on brk.appartementsrechtsplitsing(app_rechtsplitstype_code);
create index on brk.appartementsrechtsplitsing(id);
create index on brk.bijpijling(id);
create index on brk.bijpijling(volgnummer);
create index on brk.c_aanduidinggeslacht(code);
create index on brk.c_aardaantekening(code);
create index on brk.c_aardstukdeel(code);
create index on brk.c_aardzakelijkrecht(code);
create index on brk.c_akrkadastralegemeentecode(code);
create index on brk.c_appartementsrechtsplitstype(code);
create index on brk.c_beschikkingsbevoegdheid(code);
create index on brk.c_burgerlijkestaat(code);
create index on brk.c_cultuurcodebebouwd(code);
create index on brk.c_cultuurcodeonbebouwd(code);
create index on brk.c_gbaaanduidinggeslacht(code);
create index on brk.c_gbaaanduidingnaamgebruik(code);
create index on brk.c_gbaland(code);
create index on brk.c_kadastralegemeente(code);
create index on brk.c_land(code);
create index on brk.c_nhrrechtsvorm(code);
create index on brk.c_rechtsvorm(code);
create index on brk.c_registercode(code);
create index on brk.c_samenwerkingsverband(code);
create index on brk.c_soortgrootte(code);
create index on brk.c_soortregister(code);
create index on brk.kadastraal_adres(adresseerbaar_object_id);
create index on brk.kadastraal_adres(cultuurbebouwd_code);
create index on brk.kadastraal_object(id);
create index on brk.kadastraal_object(id, volgnummer);
create index on brk.kadastraal_object(volgnummer);
create index on brk.kadastraalobject_onderzoek(kadastraalobject_id);
create index on brk.kadastraalobject_onderzoek(kadastraalobject_id, kadastraalobject_volgnummer);
create index on brk.kadastraalobject_onderzoek(kadastraalobject_volgnummer);
create index on brk.perceelnummer(id);
create index on brk.perceelnummer(volgnummer);
create index on brk.stuk(id);
create index on brk.stuk(registercode_code);
create index on brk.stuk(soortregister_code);
create index on brk.stukdeel(aardstukdeel_code);
create index on brk.stukdeel(identificatie);
create index on brk.stukdeel(stuk_id);
create index on brk.subject(aanduidingnaamgebruik_code);
create index on brk.subject(beschikkingsbevoegdheid_code);
create index on brk.subject(geboorteland_code);
create index on brk.subject(geslacht_code);
create index on brk.subject(identificatie);
create index on brk.subject(kad_geboorteland_code);
create index on brk.subject(kad_geslacht_code);
create index on brk.subject(kad_rechtsvorm_code);
create index on brk.subject(landwaarnaarvertrokken_code);
create index on brk.subject(rechtsvorm_code);
create index on brk.subject_postadres(buitenland_land_code);
create index on brk.subject_postadres(subject_id);
create index on brk.subject_woonadres(buitenland_land_code);
create index on brk.subject_woonadres(subject_id);
create index on brk.tenaamstelling(burgerlijkestaat_code);
create index on brk.tenaamstelling(id);
create index on brk.tenaamstelling(identificatie);
create index on brk.tenaamstelling(van_id);
create index on brk.tenaamstelling(van_persoon_identificatie);
create index on brk.tenaamstelling(verkregen_namens_code);
create index on brk.tenaamstelling_isgebaseerdop(stukdeel_identificatie);
create index on brk.tenaamstelling_isgebaseerdop(tenaamstelling_id);
create index on brk.tenaamstelling_onderzoek(tenaamstelling_id);
create index on brk.zakelijkrecht(aardzakelijkrecht_code);
create index on brk.zakelijkrecht(betrokken_bij);
create index on brk.zakelijkrecht(id);
create index on brk.zakelijkrecht(ontstaan_uit);
create index on brk.zakelijkrecht(rust_op_kadastraalobj_volgnr);
create index on brk.zakelijkrecht(rust_op_kadastraalobject_id);
create index on brk.import_aardaantekening(code);
create index on brk.import_cultuur_bebouwd(code);
create index on brk.import_cultuur_onbebouwd(code);
