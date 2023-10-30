SELECT * FROM public.synced_schemas

-- SELECT EXISTS (
--     SELECT FROM 
--         pg_tables
--     WHERE 
--         schemaname = 'public' AND 
--         tablename  = 'synced_schemas'
--     )
