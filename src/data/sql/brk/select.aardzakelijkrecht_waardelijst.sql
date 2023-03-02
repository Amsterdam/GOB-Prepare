WITH akr_codes(aard_code, akr_code) AS (VALUES ('1', 'BK'),
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
                                               ('24', 'BPG'))
SELECT
    azt.*,
    c.akr_code as akrcode
FROM brk_prep.aardzakelijkrecht_waardelijst_import azt
LEFT JOIN akr_codes c ON c.aard_code = azt.code