CREATE SCHEMA IF NOT EXISTS brk_metadata;

CREATE TABLE IF NOT EXISTS brk_metadata.last_source_id (
  id SERIAL,
  collection VARCHAR(50) NOT NULL,
  id_column VARCHAR(63) NOT NULL,
  last_id INTEGER NOT NULL,
  date_registered TIMESTAMP NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO brk_metadata.last_source_id (
  collection,
  id_column,
  last_id,
  date_registered
)
SELECT
  'aantekening_kadastraal_object' AS collection,
  'nrn_atg_id' AS id_column,
  max(nrn_atg_id) AS last_id,
  now() AS date_registered
FROM brk_prep.aantekening_kadastraal_object;

INSERT INTO brk_metadata.last_source_id (
  collection,
  id_column,
  last_id,
  date_registered
)
SELECT
  'aantekening_recht' AS collection,
  'nrn_atg_id' AS id_column,
  max(nrn_atg_id) AS last_id,
  now() AS date_registered
FROM brk_prep.aantekening_recht;

INSERT INTO brk_metadata.last_source_id (
  collection,
  id_column,
  last_id,
  date_registered
)
SELECT
  'stukdeel' AS collection,
  'nrn_sdl_id' AS id_column,
  max(nrn_sdl_id) AS last_id,
  now() AS date_registered
FROM brk_prep.stukdeel;

INSERT INTO brk_metadata.last_source_id (
  collection,
  id_column,
  last_id,
  date_registered
)
SELECT
  'kadastraal_subject' AS collection,
  'nrn_sjt_id' AS id_column,
  max(nrn_sjt_id) AS last_id,
  now() AS date_registered
FROM brk_prep.kadastraal_subject;
