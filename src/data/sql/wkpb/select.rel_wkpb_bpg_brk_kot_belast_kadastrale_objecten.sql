SELECT bpg.id,
       bpg.src_id,
       bpg.src_volgnummer,
       bpg.bronwaarde,
       kot.brk_kot_id          AS dst_id,
       max(kot.nrn_kot_volgnr) AS dst_volgnummer
FROM wkpb_prep.beperkingen bpg
INNER JOIN brk_prepared.kadastraal_object kot
        ON bpg.bronwaarde = kot.kadastrale_aanduiding
GROUP BY bpg.id, bpg.src_id, bpg.src_volgnummer, bpg.bronwaarde, kot.brk_kot_id