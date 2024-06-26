from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobcore.exceptions import GOBException
from gobprepare.cloner.oracle_to_postgres import OracleToPostgresCloner
from tests.fixtures import random_string


class TestOracleToPostgresCloner(TestCase):

    def setUp(self):
        self.oracle_datastore_mock = MagicMock()
        self.sql_datastore_mock = MagicMock()
        self.src_schema = random_string()
        self.dst_schema = random_string()

        self.cloner = OracleToPostgresCloner(
            self.oracle_datastore_mock,
            self.src_schema,
            self.sql_datastore_mock,
            self.dst_schema,
            {}
        )

    def test_init_no_ignore_tables(self):
        # Test empty dict
        cloner = OracleToPostgresCloner(self.oracle_datastore_mock, self.src_schema, self.sql_datastore_mock,
                                        self.dst_schema, {})

        self.assertEqual([], cloner._ignore_tables)

        # Test None
        cloner = OracleToPostgresCloner(self.oracle_datastore_mock, self.src_schema, self.sql_datastore_mock,
                                        self.dst_schema, None)
        self.assertEqual([], cloner._ignore_tables)

    def test_init_with_ignore_tables(self):
        config = {
            "ignore": [
                "table_a",
                "table_b",
            ],
        }
        cloner = OracleToPostgresCloner(self.oracle_datastore_mock, self.src_schema, self.sql_datastore_mock,
                                        self.dst_schema, config)

        self.assertEqual(config["ignore"], cloner._ignore_tables)

    def test_init_with_ignore_and_include(self):
        config = {
            "ignore": ["table_a"],
            "include": ["table_b"],
        }
        with self.assertRaises(GOBException):
            OracleToPostgresCloner(self.oracle_datastore_mock, self.src_schema, self.sql_datastore_mock,
                                   self.dst_schema, config)

    def test_init_with_include_tables(self):
        config = {
            "include": ["table_a", "table_b"]
        }
        cloner = OracleToPostgresCloner(self.oracle_datastore_mock, self.src_schema, self.sql_datastore_mock,
                                        self.dst_schema, config)
        self.assertEqual(config["include"], cloner._include_tables)

    def test_filter_tables_no_filter(self):
        self.cloner._ignore_tables = []
        self.cloner._include_tables = []

        input = [random_string() for _ in range(10)]

        self.assertEqual(input, self.cloner._filter_tables(input))

    def test_filter_tables_ignore(self):
        self.cloner._include_tables = []
        self.cloner._ignore_tables = ['^PREFIX_.*', 'TABLE_NAME', '^OTHERTABLE$']

        input = [
            {'table_name': 'PREFIX_SOMETHING'},
            {'table_name': 'SPREFIX_OTHER'},
            {'table_name': 'TABLE_NAME'},
            {'table_name': 'OTHER_TABLE_NAME_NOT_IGNORED'},
            {'table_name': 'OTHERTABLE'},
            {'table_name': 'OTHERTABLE_NOT_IGNORED'},
        ]

        expected_result = [
            {'table_name': 'SPREFIX_OTHER'},
            {'table_name': 'OTHER_TABLE_NAME_NOT_IGNORED'},
            {'table_name': 'OTHERTABLE_NOT_IGNORED'},
        ]

        self.assertEqual(expected_result, self.cloner._filter_tables(input))

    def test_filter_tables_include(self):
        self.cloner._ignore_tables = []
        self.cloner._include_tables = ['^PREFIX_.*', 'TABLE_NAME', '^OTHERTABLE$']

        input = [
            {'table_name': 'PREFIX_SOMETHING'},
            {'table_name': 'SPREFIX_OTHER'},
            {'table_name': 'TABLE_NAME'},
            {'table_name': 'OTHER_TABLE_NAME_NOT_IGNORED'},
            {'table_name': 'OTHERTABLE'},
            {'table_name': 'OTHERTABLE_NOT_IGNORED'},
        ]

        expected_result = [
            {'table_name': 'PREFIX_SOMETHING'},
            {'table_name': 'TABLE_NAME'},
            {'table_name': 'OTHERTABLE'},
        ]

        self.assertEqual(expected_result, self.cloner._filter_tables(input))

    def test_read_source_table_names(self):
        self.cloner._filter_tables = MagicMock(side_effect=lambda x: x)
        self.cloner._src_datastore.read.return_value = [{'table_name': 'tableA'}, {'table_name': 'tableB'}]

        self.assertEqual(['tableA', 'tableB'], self.cloner.read_source_table_names())

        self.cloner._src_datastore.read.assert_called_with(
            f"SELECT table_name FROM all_tables WHERE owner='{self.src_schema}' ORDER BY table_name"
        )
        self.cloner._filter_tables.assert_called_with(self.cloner._src_datastore.read.return_value)

    @patch("gobprepare.cloner.oracle_to_postgres.get_postgres_column_definition")
    def test_get_source_table_definition(self, mock_get_postgres_column_definition):
        mock_get_postgres_column_definition.return_value = "COLUMNDEF()"
        self.cloner._src_datastore.read.return_value = [
            {
                "column_name": "columnA",
                "data_type": "some_int",
                "data_length": 8,
                "data_precision": None,
                "data_scale": None
            },
            {
                "column_name": "columnB",
                "data_type": "some_number",
                "data_length": None,
                "data_precision": 5,
                "data_scale": 2
            },
        ]
        expected_result = [
            ("columnA", "COLUMNDEF()"),
            ("columnB", "COLUMNDEF()"),
        ]
        table_name = "some_table_name"

        result = self.cloner._get_source_table_definition(table_name)
        self.assertEqual(expected_result, result)

        mock_get_postgres_column_definition.assert_any_call("some_int", 8, None, None)
        mock_get_postgres_column_definition.assert_any_call("some_number", None, 5, 2)
        self.cloner._src_datastore.read.assert_called_with(
            f"SELECT column_name, data_type, data_length, data_precision, data_scale FROM all_tab_columns WHERE "
             f"owner='{self.src_schema}' AND table_name='{table_name}' ORDER BY column_id",
        )

    def test_prepare_destination_database(self):
        self.cloner._get_destination_schema_definition = MagicMock(
            return_value=["tabledef_a", "tabledef_b", "tabledef_c"]
        )
        self.cloner._create_destination_table = MagicMock()
        self.cloner._prepare_destination_database()

        self.cloner._create_destination_table.assert_has_calls([
            call("tabledef_a"), call("tabledef_b"), call("tabledef_c")
        ])

    @patch("gobprepare.cloner.oracle_to_postgres.create_table_columnar_query")
    def test_create_destination_table(self, mock_create_table):
        table_definition = ("tablename", [('id', 'INT'), ('first_name', 'VARCHAR(20)'), ('last_name', 'VARCHAR(20)')])
        self.cloner._create_destination_table(table_definition)

        self.cloner._dst_datastore.drop_table.assert_called_with(f"{self.dst_schema}.tablename")

        mock_create_table.assert_called_with(
            self.cloner._dst_datastore,
            f"{self.dst_schema}.tablename",
            "id INT NULL,first_name VARCHAR(20) NULL,last_name VARCHAR(20) NULL",
        )
        self.cloner._dst_datastore.execute.assert_called_with(mock_create_table.return_value)

    def test_get_destination_schema_definition(self):
        self.cloner.read_source_table_names = MagicMock(return_value=["table_a", "table_b"])
        self.cloner._get_source_table_definition = MagicMock(return_value="some table definition")

        expected_result = [("table_a", "some table definition"), ("table_b", "some table definition")]

        # Call twice to trigger both paths
        self.assertEqual(expected_result, self.cloner._get_destination_schema_definition())
        self.assertEqual(expected_result, self.cloner._get_destination_schema_definition())
        self.cloner._get_source_table_definition.assert_has_calls([call("table_a"), call("table_b")])
        self.cloner.read_source_table_names.assert_called_once()

    def test_copy_data(self):
        self.cloner._get_destination_schema_definition = MagicMock(return_value=[
            ("tabledef_a", []), ("tabledef_b", []), ("tabledef_c", [])
        ])
        self.cloner._copy_table_data = MagicMock(return_value=4)
        self.assertEqual(12, self.cloner._copy_data())
        self.cloner._copy_table_data.has_calls([
            call("tabledef_a"),
            call("tabledef_b"),
            call("tabledef_c"),
        ])

    def test_list_to_chunks(self):
        testcases = [
            (([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3), [{"min": None, "max": 4}, {"min": 4, "max": 7}, {"min": 7, "max": 10}, {"min": 10, "max": None}]),
            (([1, 2, 3], 1), [{"min": None, "max": 2}, {"min": 2, "max": 3}, {"min": 3, "max": None}]),
            (([], 4), []),
            (([1, 3], 2), [{"min": None, "max": None}]),
            (([1, 3], 5), [{"min": None, "max": None}]),
        ]

        for input, result in testcases:
            lst, chunk_size = input
            self.assertEqual(result, self.cloner._list_to_chunks(lst, chunk_size))

        with self.assertRaises(AssertionError):
            self.cloner._list_to_chunks([], 0)

        with self.assertRaises(AssertionError):
            self.cloner._list_to_chunks([], -1)

    def test_get_id_columns_for_table(self):
        self.cloner._id_columns = {
            "table_a": ["a_id", "a_id2"],
            "_defaults": [
                ["id", "volgnummer"],
                ["id"],
            ]
        }
        self.assertEqual(["a_id", "a_id2"], self.cloner._get_id_columns_for_table("schema.table_a", []))

        with self.assertRaises(GOBException):
            self.cloner._get_id_columns_for_table("table_b", [('somethingelse', 'varchar')])

        self.assertEqual(["id", "volgnummer"], self.cloner._get_id_columns_for_table("table_b", [("id", "int"), ("volgnummer", "int")]))
        self.assertEqual(["id"], self.cloner._get_id_columns_for_table("table_b", [("id", "int")]))

    def test_get_ids_for_table(self):
        self.cloner._get_id_columns_for_table = MagicMock(return_value=['id', 'id2'])
        expected_query = f"SELECT id FROM tableName ORDER BY id"
        self.cloner._src_datastore.read.return_value = [{"id": 224}, {"id": 2904}, {"id": 920}]
        expected_result = [224, 2904, 920]

        self.assertEqual(expected_result, self.cloner._get_ids_for_table('tableName', "id"))
        self.cloner._src_datastore.read.assert_called_with(expected_query)

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    def test_copy_table_data(self, mock_logger):
        # Mock result as list of 0's
        self.cloner._src_datastore.read.side_effect = [
            self.cloner.READ_BATCH_SIZE * [0] + (self.cloner.READ_BATCH_SIZE - 1) * [0]
        ]

        self.cloner._get_select_list_for_table_definition = MagicMock(return_value="colA, colB, colC")
        self.cloner._insert_rows = MagicMock()
        self.cloner._get_order_by_clause = MagicMock(return_value="ORDERBYCLAUSE")
        self.cloner._get_ids_for_table = MagicMock(return_value='list of ids')
        self.cloner._get_id_columns_for_table = MagicMock()
        self.cloner._list_to_chunks = MagicMock(return_value=[{"min": None, "max": None}])

        self.assertEqual(self.cloner.READ_BATCH_SIZE * 2 - 1,
                         self.cloner._copy_table_data(
                             ("tableName", [('colA', 'int'), ('colB', 'varchar'), ('colC', 'varchar')]))
                         )

        self.cloner._src_datastore.read.assert_has_calls([
            call(f"SELECT /*+ PARALLEL */ colA, colB, colC FROM (  "
                 f"SELECT colA,colB,colC FROM {self.src_schema}.tableName )"),
        ])

        mock_logger.info.assert_called_once()
        self.cloner._get_ids_for_table(f"{self.cloner._src_schema}.tableName")
        self.cloner._get_id_columns_for_table.assert_called_with(
            f"{self.cloner._src_schema}.tableName",
            [('colA', 'int'), ('colB', 'varchar'), ('colC', 'varchar')]
        )
        self.cloner._list_to_chunks.assert_called_with('list of ids', self.cloner.READ_BATCH_SIZE)
        self.cloner._dst_datastore.execute.assert_called_with(f"ANALYZE {self.dst_schema}.tableName")

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    def test_copy_table_data_chunk_minmax(self, mock_logger):
        self.cloner._get_select_list_for_table_definition = MagicMock(return_value='cols')
        self.cloner._insert_rows = MagicMock()
        self.cloner._get_order_by_clause = MagicMock()
        self.cloner._get_ids_for_table = MagicMock()
        self.cloner._get_id_columns_for_table = MagicMock(return_value=["order_column", "other_order_column"])
        self.cloner._list_to_chunks = MagicMock()

        cases = [
            ([{'min': None, 'max': 10}], " WHERE   order_column < '10'"),
            ([{'min': 10, 'max': None}], " WHERE order_column >= '10'  "),
            ([{'min': 10, 'max': 100}], " WHERE order_column >= '10' AND order_column < '100'"),
            ([{'min': None, 'max': None}], " "),
        ]
        expected_query_start = f"SELECT /*+ PARALLEL */ cols FROM (  " \
            f"SELECT col_name FROM {self.cloner._src_schema}.tableName"

        for arg, where_clause in cases:
            self.cloner._list_to_chunks.return_value = arg
            self.cloner._copy_table_data(('tableName', [('col_name', 'col_type')]))
            expected_query = expected_query_start + where_clause + ')'
            self.cloner._src_datastore.read.assert_called_with(expected_query)

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    @patch("gobprepare.cloner.oracle_to_postgres.DEBUG", True)
    def test_copy_table_data_with_debug(self, mock_logger):
        # Mock result as list of 0's
        self.cloner._src_datastore.side_effect = [self.cloner.READ_BATCH_SIZE * [0],
                                             (self.cloner.READ_BATCH_SIZE - 1) * [0]]

        self.cloner._get_select_list_for_table_definition = MagicMock(return_value="colA, colB, colC")
        self.cloner._insert_rows = MagicMock()
        self.cloner._get_ids_for_table = MagicMock()
        self.cloner._get_id_columns_for_table = MagicMock()
        self.cloner._list_to_chunks = MagicMock(return_value=[{"min": None, "max": None}])
        self.cloner._copy_table_data(("tableName", [('colA', 'int'), ('colB', 'varchar'), ('colC', 'varchar')]))

        self.assertEqual(2, mock_logger.info.call_count)

    def test_insert_rows(self):
        self.cloner.WRITE_BATCH_SIZE = 2

        table_definition = ("table_name", [
            ("id", "INT"),
            ("name", "VARCHAR(20)"),
        ])
        row_data = [
            {"name": "Sheldon", "id": 3},
            {"name": "Leonard", "id": 4},
            {"name": "Amy", "id": 7},
            {"name": "Rajesh", "id": 1},
            {"name": "Howard", "id": 9},
            {"name": "Penny", "id": 5},
            {"name": "Kripke", "id": 2},
        ]
        self.cloner._insert_rows(table_definition, row_data)

        full_table_name = f"{self.dst_schema}.table_name"
        # The rows should be divided in chunks of size WRITE_BATCH_SIZE and the values should be in order according to
        # table_definition
        self.cloner._dst_datastore.write_rows.assert_has_calls([
            call(full_table_name, [[3, "Sheldon"], [4, "Leonard"]]),
            call(full_table_name, [[7, "Amy"], [1, "Rajesh"]]),
            call(full_table_name, [[9, "Howard"], [5, "Penny"]]),
            call(full_table_name, [[2, "Kripke"]]),
        ])

    def test_get_select_list_for_table_definition(self):
        table_definition = ("table_name", [
            ("id", "INT"),
            ("first_name", "VARCHAR(20)"),
            ("last_name", "VARCHAR(21)"),
        ])

        self.cloner._get_select_expr = MagicMock()
        self.cloner._get_select_expr.side_effect = ["COLUMN_A", "COLUMN_B", "COLUMN_C"]

        expected_result = "COLUMN_A,COLUMN_B,COLUMN_C"
        result = self.cloner._get_select_list_for_table_definition(table_definition)
        self.assertEqual(expected_result, result)
        self.cloner._get_select_expr.assert_has_calls([
            call(("id", "INT")),
            call(("first_name", "VARCHAR(20)")),
            call(("last_name", "VARCHAR(21)")),
        ])

    def test_get_select_expr(self):
        test_cases = [
            (("id", "INT"), "id"),
            (("first_name", "VARCHAR(20)"), "first_name"),
            (("location", "GEOMETRY"), "SDO_UTIL.TO_WKTGEOMETRY(location) AS location"),
            (("blobby", "BYTEA"), "DBMS_LOB.SUBSTR(blobby) AS blobby"),
        ]

        for arg, result in test_cases:
            self.assertEqual(result, self.cloner._get_select_expr(arg))

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    def test_clone(self, mock_logger):
        self.cloner._prepare_destination_database = MagicMock()
        self.cloner._copy_data = MagicMock(return_value=824802)

        self.cloner.clone()
        self.cloner._prepare_destination_database.assert_called_once()
        self.cloner._copy_data.assert_called_once()

        # Assert the total number of rows is logged
        args = mock_logger.info.call_args[0]
        self.assertTrue("824802" in args[0])
