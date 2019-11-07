from unittest import TestCase

from gobprepare.utils.sql import get_create_table_sql


class TestSQL(TestCase):

    def test_create_table_with_primary_key(self):
        schema = 'schema'
        table_name = 'table_name'
        description = 'description'
        meta_fields = {
            'a': {'name': 'a', 'description': 'just a.', 'type': {'name': 'String'}},
            'b': {'name': 'b', 'description': 'just b.', 'type': {'name': 'Int'}},
            'c': {'name': 'c', 'description': 'just c.', 'type': {'name': 'DateTime'}},
        }
        field_names = ["a", "b"]
        primary_key = "b"

        result = get_create_table_sql(schema, table_name, description, meta_fields, field_names, primary_key)
        self.assertEqual(result,
"""
DROP TABLE IF EXISTS "schema"."table_name" CASCADE;
-- TRUNCATE TABLE "schema"."table_name";
CREATE TABLE IF NOT EXISTS "schema"."table_name"
(
  "a" character varying,
  "b" integer
  ,PRIMARY KEY (b)
);
COMMIT;

-- Table and Column comments
COMMENT ON TABLE  "schema"."table_name" IS 'description';
COMMENT ON COLUMN "schema"."table_name"."a" IS 'just a.';
COMMENT ON COLUMN "schema"."table_name"."b" IS 'just b.'
"""
    )

    def test_create_table_no_primary_key(self):
        schema = 'schema'
        table_name = 'table_name'
        description = 'description'
        meta_fields = {
            'a': {'name': 'a', 'description': 'just a.', 'type': {'name': 'String'}},
            'b': {'name': 'b', 'description': 'just b.', 'type': {'name': 'Int'}},
            'c': {'name': 'c', 'description': 'just c.', 'type': {'name': 'DateTime'}},
        }
        field_names = ["a", "b"]
        primary_key = None

        result = get_create_table_sql(schema, table_name, description, meta_fields, field_names, primary_key)
        self.assertEqual(result,
"""
DROP TABLE IF EXISTS "schema"."table_name" CASCADE;
-- TRUNCATE TABLE "schema"."table_name";
CREATE TABLE IF NOT EXISTS "schema"."table_name"
(
  "a" character varying,
  "b" integer

);
COMMIT;

-- Table and Column comments
COMMENT ON TABLE  "schema"."table_name" IS 'description';
COMMENT ON COLUMN "schema"."table_name"."a" IS 'just a.';
COMMENT ON COLUMN "schema"."table_name"."b" IS 'just b.'
"""
    )
