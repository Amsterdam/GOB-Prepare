CREATE OR REPLACE FUNCTION brp_build_date_json(datestr varchar)
RETURNS jsonb AS $$
BEGIN
    RETURN
      CASE
        WHEN datestr IS NULL THEN NULL
        WHEN datestr = '00000000' THEN JSONB_BUILD_OBJECT(
          'datum', '0001-01-01',
          'jaar', '0001',
          'maand', '01',
          'dag', '01')
        ELSE JSONB_BUILD_OBJECT(
          'datum', CONCAT_WS(
            '-',
            substring(datestr, 1, 4),
            substring(datestr, 5, 2),
            substring(datestr, 7, 2)
          ),
          'jaar', substring(datestr, 1, 4),
          'maand', substring(datestr, 5, 2),
          'dag', substring(datestr, 7, 2)
        )
      END;
END;
$$ LANGUAGE plpgsql;
