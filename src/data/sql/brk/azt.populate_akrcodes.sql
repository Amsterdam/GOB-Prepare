-- Add column to fill
ALTER TABLE brk_prep.aardzakelijkrecht_waardelijst
    ADD akrcode varchar(5) NULL;

-- Add AKR codes
UPDATE brk_prep.aardzakelijkrecht_waardelijst azt
SET akrcode = COALESCE(codes.akr, '?')
FROM (SELECT aztw.code, c.akr
      FROM brk_prep.aardzakelijkrecht_waardelijst aztw
               LEFT JOIN (VALUES ('1', 'BK'),
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
                                 ('24', 'BPG')
      ) c(brk, akr) ON aztw.code = c.brk) codes(brk, akr)
WHERE codes.brk=azt.code;
