-- Add column to fill
ALTER TABLE brk_prep.aardzakelijkrecht_waardelijst
    ADD akrcode varchar(5) NULL;


CREATE TABLE brk_prep.akr_codes AS
SELECT *
FROM (VALUES ('1', 'BK'),
             ('2', 'VE'),
             ('3', 'EP'),
             ('4', 'GB'),
             ('5', 'GR'),
             ('7', 'OS'),
             ('9', 'OVR'),
             ('10', 'BP'),
             ('11', 'SM'),
             ('12', 'VG'),
             ('13', 'EO'),
             ('14', 'OL'),
             ('18', 'OV'),
             ('20', 'AA'),
             ('21', 'BB'),
             ('22', 'BR'),
             ('23', 'OLG'),
             ('24', 'BPG')) c (brk_code, akr_code);

-- Add AKR codes
UPDATE brk_prep.aardzakelijkrecht_waardelijst azt
SET akrcode = COALESCE(c.akr_code, '?')
FROM brk_prep.akr_codes c
WHERE c.brk_code = azt.code;
