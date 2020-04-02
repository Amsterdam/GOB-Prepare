from collections import KeysView

from unittest import TestCase
from unittest.mock import patch, call, ANY, MagicMock

from gobcore.exceptions import GOBException
from gobprepare.importers.api_importer import SqlAPIImporter


dst_datastore = MagicMock()
config =     {
  "description": "the description",
  "meta_type": "the meta_type",
  "schema": "the schema",
  "destination": "the destination"
}
query = "the query"

test_entities = [
    {'node': {'a': '1', 'b': 44}},
    {'node': {'a': '2', 'b': 55}},
    {'node': {'a': '3', 'b': 66}},
]
test_field_names = ["a", "b"]
test_meta_data = [
    {
      "data": {
        "__type": {
          "name": "the meta_type",
          "fields": [
            {
              "name": "a",
              "description": "A",
              "type": {
                "name": "String"
              }
            },
            {
              "name": "b",
              "description": "B",
              "type": {
                "name": "Int"
              }
            },
          ]
        }
      }
    }
]
test_meta_fields = {
    'a': {'name': 'a', 'description': 'A', 'type': {'name': 'String'}},
    'b': {'name': 'b', 'description': 'B', 'type': {'name': 'Int'}},
}


class TestSqlAPIImporter(TestCase):

    def setUp(self):
        self.importer = SqlAPIImporter(dst_datastore, config, query)

    def test_init(self):
        assert self.importer._dst_datastore == dst_datastore
        assert self.importer.query == query
        assert self.importer.schema == config['schema']
        assert self.importer.table_name == config['destination']
        assert self.importer.description == config['description']
        assert self.importer.meta_type == config['meta_type']

    @patch("gobprepare.importers.api_importer.SqlAPIImporter.get_entities", return_value=iter(test_entities))
    @patch("gobprepare.importers.api_importer.SqlAPIImporter.get_meta_data", return_value=iter(test_meta_data))
    @patch("gobprepare.importers.api_importer.SqlAPIImporter.import_data", return_value=3)
    def test_import_api(self, mock_import_data, mock_get_meta_data, mock_get_entities):
        self.assertEqual(self.importer.import_api(), 3)  # number of imported records
        mock_get_entities.assert_called_with()
        mock_get_meta_data.assert_called_with()
        mock_import_data.assert_called_with(mock_get_entities.return_value, mock_get_meta_data.return_value)

    @patch("gobprepare.importers.api_importer.GraphQLStreaming", return_value=iter(test_entities))
    def test_get_entities(self, mock_graphql):
        self.assertEqual(list(self.importer.get_entities()), test_entities)

    @patch("gobprepare.importers.api_importer.GraphQLStreaming", return_value=iter([]))
    def test_get_entities_empty(self, mock_graphql):
        self.assertEqual(list(self.importer.get_entities()), [])

    @patch("gobprepare.importers.api_importer.SqlAPIImporter.get_meta_query", return_value="the meta_query")
    @patch("gobprepare.importers.api_importer.GraphQL", return_value=iter(test_meta_data))
    def test_get_meta_data(self, mock_graphql, mock_get_meta_query):
        self.assertEqual(list(self.importer.get_meta_data()), test_meta_data)
        mock_get_meta_query.assert_called_with("the meta_type")

    @patch(
        "gobprepare.importers.api_importer.SqlAPIImporter.get_meta_fields",
        return_value={
            'a': {'name': 'a', 'description': 'A', 'type': {'name': 'String'}},
            'b': {'name': 'b', 'description': 'B', 'type': {'name': 'Int'}},
        }
    )
    @patch("gobprepare.importers.api_importer.SqlAPIImporter.create_table")
    @patch("gobprepare.importers.api_importer.SqlAPIImporter.import_entities", return_value=3)
    def test_import_data(
            self,
            mock_import_entities,
            mock_create_table,
            mock_get_meta_fields
        ):
        iter_test_meta_data = iter(test_meta_data)
        self.assertEqual(self.importer.import_data(iter(test_entities), iter_test_meta_data), 3)  # number of imported records
        dst_datastore.create_schema.assert_called_with(config['schema'])
        mock_get_meta_fields.assert_called_with(iter_test_meta_data)
        mock_create_table.assert_called_with(test_meta_fields, KeysView(test_field_names))
        mock_import_entities.assert_called_with(ANY, KeysView(test_field_names))

    def test_import_data_empty(self):
        with self.assertRaises(GOBException):
            self.importer.import_data(iter([]), iter(test_meta_data))

    def test_get_meta_query(self):
        test_meta_query = '''
{
  __type(name: "the meta_type") {
    name
    fields {
      name
      description
      type {
        name
      }
    }
  }
}
'''
        self.assertEqual(self.importer.get_meta_query("the meta_type"), test_meta_query)

    def test_get_meta_fields(self):
        self.assertEqual(self.importer.get_meta_fields(test_meta_data), test_meta_fields)

    def test_get_field_names(self):
        self.assertEqual(list(self.importer.get_field_names(iter(test_entities))), test_field_names)

    def test_get_field_names_empty(self):
        with self.assertRaises(GOBException):
            self.importer.get_field_names(iter([]))

    @patch("gobprepare.importers.api_importer.get_create_table_sql", return_value="foo")
    def test_create_table(self, mock_get_create_table_sql):
        self.importer.create_table(test_meta_fields, test_field_names)
        mock_get_create_table_sql.assert_called_with(
            schema=self.importer.schema,
            table_name=self.importer.table_name,
            description=self.importer.description,
            meta_fields=test_meta_fields,
            field_names=test_field_names,
        )
        dst_datastore.execute.assert_called_with("foo")

    @patch("gobprepare.importers.api_importer.SqlAPIImporter.write_rows")
    @patch("gobprepare.importers.api_importer.WRITE_BATCH_SIZE", 2)
    def test_import_entities(self, mock_write_rows):
        self.assertEqual(self.importer.import_entities(iter(test_entities), test_field_names), 3)
        mock_write_rows.assert_has_calls(
            [call([['1', 44], ['2', 55]]), call([['3', 66]])]
        )

    def test_write_rows(self):
        rows = [['1', 44], ['2', 55]]
        self.importer.write_rows(rows)
        dst_datastore.write_rows.assert_called_with('"the schema"."the destination"', rows)
