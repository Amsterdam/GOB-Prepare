-- Copies the zrt_kot_wip table. The zrt_kot_wip table is a default Postgres table because of all the update
-- actions involved. This query copies that table into a columnar table, if citus is available.
SELECT * FROM brk_prep.zrt_kot_wip