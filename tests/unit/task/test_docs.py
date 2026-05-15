import unittest
from decimal import Decimal
from unittest import mock

from dbt.task.docs import generate


class GenerateTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.manifest = mock.MagicMock()
        self.patcher = mock.patch("dbt.task.docs.generate.get_unique_id_mapping")
        self.mock_get_unique_id_mapping = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def map_uids(self, effects):
        results = {generate.CatalogKey(db, sch, tbl): uid for db, sch, tbl, uid in effects}
        self.mock_get_unique_id_mapping.return_value = results, {}

    def generate_catalog_dict(self, columns):
        nodes, sources = generate.Catalog(columns).make_unique_id_map(self.manifest)
        result = generate.CatalogResults(
            nodes=nodes,
            sources=sources,
            errors=None,
        )
        return result.to_dict(omit_none=False)["nodes"]

    def test__unflatten_empty(self):
        columns = {}
        expected = {}
        self.map_uids([])

        result = self.generate_catalog_dict(columns)

        self.mock_get_unique_id_mapping.assert_called_once_with(self.manifest)
        self.assertEqual(result, expected)

    def test__unflatten_one_column(self):
        columns = [
            {
                "column_comment": None,
                "column_index": Decimal("1"),
                "column_name": "id",
                "column_type": "integer",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
            }
        ]
        expected = {
            "test.model.test_table": {
                "metadata": {
                    "owner": None,
                    "comment": None,
                    "name": "test_table",
                    "type": "BASE TABLE",
                    "schema": "test_schema",
                    "database": "test_database",
                },
                "columns": {
                    "id": {"type": "integer", "comment": None, "index": 1, "name": "id"},
                },
                "stats": {
                    "has_stats": {
                        "id": "has_stats",
                        "label": "Has Stats?",
                        "value": False,
                        "description": "Indicates whether there are statistics for this table",
                        "include": False,
                    },
                },
                "unique_id": "test.model.test_table",
            },
        }
        self.map_uids([("test_database", "test_schema", "test_table", "test.model.test_table")])

        result = self.generate_catalog_dict(columns)

        self.mock_get_unique_id_mapping.assert_called_once_with(self.manifest)
        self.assertEqual(result, expected)

    def test__unflatten_multiple_schemas_dbs(self):
        columns = [
            {
                "column_comment": None,
                "column_index": Decimal("1"),
                "column_name": "id",
                "column_type": "integer",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("2"),
                "column_name": "name",
                "column_type": "text",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("1"),
                "column_name": "id",
                "column_type": "integer",
                "table_comment": None,
                "table_name": "other_test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("2"),
                "column_name": "email",
                "column_type": "character varying",
                "table_comment": None,
                "table_name": "other_test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("1"),
                "column_name": "id",
                "column_type": "integer",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "other_test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("2"),
                "column_name": "name",
                "column_type": "text",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "other_test_schema",
                "table_type": "BASE TABLE",
                "table_database": "test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("1"),
                "column_name": "id",
                "column_type": "integer",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "other_test_database",
                "table_owner": None,
            },
            {
                "column_comment": None,
                "column_index": Decimal("2"),
                "column_name": "name",
                "column_type": "text",
                "table_comment": None,
                "table_name": "test_table",
                "table_schema": "test_schema",
                "table_type": "BASE TABLE",
                "table_database": "other_test_database",
                "table_owner": None,
            },
        ]
        expected = {
            "test.model.test_table": {
                "metadata": {
                    "owner": None,
                    "comment": None,
                    "name": "test_table",
                    "type": "BASE TABLE",
                    "schema": "test_schema",
                    "database": "test_database",
                },
                "columns": {
                    "id": {"type": "integer", "comment": None, "index": 1, "name": "id"},
                    "name": {
                        "type": "text",
                        "comment": None,
                        "index": 2,
                        "name": "name",
                    },
                },
                "stats": {
                    "has_stats": {
                        "id": "has_stats",
                        "label": "Has Stats?",
                        "value": False,
                        "description": "Indicates whether there are statistics for this table",
                        "include": False,
                    },
                },
                "unique_id": "test.model.test_table",
            },
            "test.model.other_test_table": {
                "metadata": {
                    "owner": None,
                    "comment": None,
                    "name": "other_test_table",
                    "type": "BASE TABLE",
                    "schema": "test_schema",
                    "database": "test_database",
                },
                "columns": {
                    "id": {"type": "integer", "comment": None, "index": 1, "name": "id"},
                    "email": {
                        "type": "character varying",
                        "comment": None,
                        "index": 2,
                        "name": "email",
                    },
                },
                "stats": {
                    "has_stats": {
                        "id": "has_stats",
                        "label": "Has Stats?",
                        "value": False,
                        "description": "Indicates whether there are statistics for this table",
                        "include": False,
                    },
                },
                "unique_id": "test.model.other_test_table",
            },
            "test.model.test_table_otherschema": {
                "metadata": {
                    "owner": None,
                    "comment": None,
                    "name": "test_table",
                    "type": "BASE TABLE",
                    "schema": "other_test_schema",
                    "database": "test_database",
                },
                "columns": {
                    "id": {"type": "integer", "comment": None, "index": 1, "name": "id"},
                    "name": {
                        "type": "text",
                        "comment": None,
                        "index": 2,
                        "name": "name",
                    },
                },
                "stats": {
                    "has_stats": {
                        "id": "has_stats",
                        "label": "Has Stats?",
                        "value": False,
                        "description": "Indicates whether there are statistics for this table",
                        "include": False,
                    },
                },
                "unique_id": "test.model.test_table_otherschema",
            },
            "test.model.test_table_otherdb": {
                "metadata": {
                    "owner": None,
                    "comment": None,
                    "name": "test_table",
                    "type": "BASE TABLE",
                    "schema": "test_schema",
                    "database": "other_test_database",
                },
                "columns": {
                    "id": {"type": "integer", "comment": None, "index": 1, "name": "id"},
                    "name": {
                        "type": "text",
                        "comment": None,
                        "index": 2,
                        "name": "name",
                    },
                },
                "stats": {
                    "has_stats": {
                        "id": "has_stats",
                        "label": "Has Stats?",
                        "value": False,
                        "description": "Indicates whether there are statistics for this table",
                        "include": False,
                    },
                },
                "unique_id": "test.model.test_table_otherdb",
            },
        }
        self.map_uids(
            [
                ("test_database", "test_schema", "test_table", "test.model.test_table"),
                (
                    "test_database",
                    "test_schema",
                    "other_test_table",
                    "test.model.other_test_table",
                ),
                (
                    "test_database",
                    "other_test_schema",
                    "test_table",
                    "test.model.test_table_otherschema",
                ),
                (
                    "other_test_database",
                    "test_schema",
                    "test_table",
                    "test.model.test_table_otherdb",
                ),
            ]
        )

        result = self.generate_catalog_dict(columns)

        self.mock_get_unique_id_mapping.assert_called_once_with(self.manifest)
        self.assertEqual(result, expected)


class TestDatabaseFlatCatalogSourceMatching(unittest.TestCase):
    """Reproduce #12966: sources with explicit database dropped when catalog has NULL database."""

    def _catalog_columns(self):
        return [
            {
                "column_comment": None,
                "column_index": Decimal("1"),
                "column_name": "id",
                "column_type": "integer",
                "table_comment": None,
                "table_name": "raw_customers",
                "table_schema": "dbt_raw_dbt",
                "table_type": "BASE TABLE",
                "table_database": None,
            }
        ]

    def _source(self, database):
        from dbt.artifacts.resources import Quoting, SourceConfig
        from dbt.artifacts.resources.types import NodeType
        from dbt.contracts.graph.nodes import SourceDefinition

        return SourceDefinition(
            columns={},
            database=database,
            description="",
            fqn=["pkg", "jaffle_raw", "raw_customers"],
            identifier="raw_customers",
            loader="",
            name="raw_customers",
            original_file_path="models/sources.yml",
            package_name="pkg",
            path="models/sources.yml",
            quoting=Quoting(),
            resource_type=NodeType.Source,
            schema="dbt_raw_dbt",
            source_description="",
            source_name="jaffle_raw",
            unique_id="source.pkg.jaffle_raw.raw_customers",
            tags=[],
            config=SourceConfig(),
        )

    def _match_sources(self, database):
        from tests.unit.utils.manifest import make_manifest

        source = self._source(database)
        manifest = make_manifest(sources=[source])
        catalog = generate.Catalog(self._catalog_columns())
        _, sources = catalog.make_unique_id_map(manifest)
        return sources

    def test_explicit_source_database_matches_null_catalog_database(self):
        """After parse normalization (dbt-core#12966), database=None matches Teradata catalog."""
        sources = self._match_sources(None)
        self.assertEqual(len(sources), 1)

    def test_non_null_source_database_does_not_match_null_catalog_database(self):
        """Strict catalog keys: non-None manifest database still fails to match NULL catalog."""
        sources = self._match_sources("dbt_raw_dbt")
        self.assertEqual(sources, {})

    def test_source_with_null_database_matches_null_catalog_database(self):
        sources = self._match_sources(None)
        self.assertEqual(len(sources), 1)
        self.assertIn("source.pkg.jaffle_raw.raw_customers", sources)
