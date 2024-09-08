import re

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.postgres.fixtures import (
    models__incremental_sql,
    models__table_sql,
    models_invalid__invalid_columns_type_sql,
    models_invalid__invalid_type_sql,
    models_invalid__invalid_unique_config_sql,
    models_invalid__missing_columns_sql,
    seeds__seed_csv,
    snapshots__colors_sql,
)

INDEX_DEFINITION_PATTERN = re.compile(r"using\s+(\w+)\s+\((.+)\)\Z")


class TestPostgresIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": models__table_sql,
            "incremental.sql": models__incremental_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"colors.sql": snapshots__colors_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
                "indexes": [
                    {"columns": ["country_code"], "unique": False, "type": "hash"},
                    {"columns": ["country_code", "country_name"], "unique": True},
                ],
            },
            "vars": {
                "version": 1,
            },
        }

    def test_table(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table"])
        assert len(results) == 1

        indexes = self.get_indexes("table", project, unique_schema)
        expected = [
            {"columns": "column_a", "unique": False, "type": "btree"},
            {"columns": "column_b", "unique": False, "type": "btree"},
            {"columns": "column_a, column_b", "unique": False, "type": "btree"},
            {"columns": "column_b, column_a", "unique": True, "type": "btree"},
            {"columns": "column_a", "unique": False, "type": "hash"},
        ]
        assert len(indexes) == len(expected)

    def test_incremental(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "incremental"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("incremental", project, unique_schema)
            expected = [
                {"columns": "column_a", "unique": False, "type": "hash"},
                {"columns": "column_a, column_b", "unique": True, "type": "btree"},
            ]
            assert len(indexes) == len(expected)

    def test_seed(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["seed"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("seed", project, unique_schema)
            expected = [
                {"columns": "country_code", "unique": False, "type": "hash"},
                {"columns": "country_code, country_name", "unique": True, "type": "btree"},
            ]
            assert len(indexes) == len(expected)

    def test_snapshot(self, project, unique_schema):
        for version in [1, 2]:
            results = run_dbt(["snapshot", "--vars", f"version: {version}"])
            assert len(results) == 1

            indexes = self.get_indexes("colors", project, unique_schema)
            expected = [
                {"columns": "id", "unique": False, "type": "hash"},
                {"columns": "id, color", "unique": True, "type": "btree"},
            ]
            assert len(indexes) == len(expected)

    def get_indexes(self, table_name, project, unique_schema):
        sql = f"""
            SELECT
              pg_get_indexdef(idx.indexrelid) as index_definition
            FROM pg_index idx
            JOIN pg_class tab ON tab.oid = idx.indrelid
            WHERE
              tab.relname = '{table_name}'
              AND tab.relnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = '{unique_schema}'
              );
        """
        results = project.run_sql(sql, fetch="all")
        return [self.parse_index_definition(row[0]) for row in results]

    def parse_index_definition(self, index_definition):
        index_definition = index_definition.lower()
        is_unique = "unique" in index_definition
        m = INDEX_DEFINITION_PATTERN.search(index_definition)
        return {
            "columns": m.group(2),
            "unique": is_unique,
            "type": m.group(1),
        }

    def assertCountEqual(self, a, b):
        assert len(a) == len(b)


class TestPostgresInvalidIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_unique_config.sql": models_invalid__invalid_unique_config_sql,
            "invalid_type.sql": models_invalid__invalid_type_sql,
            "invalid_columns_type.sql": models_invalid__invalid_columns_type_sql,
            "missing_columns.sql": models_invalid__missing_columns_sql,
        }

    def test_invalid_index_configs(self, project):
        results, output = run_dbt_and_capture(expect_pass=False)
        assert len(results) == 4
        assert re.search(r"columns.*is not of type 'array'", output)
        assert re.search(r"unique.*is not of type 'boolean'", output)
        assert re.search(r"'columns' is a required property", output)
        assert re.search(r"Database Error in model invalid_type", output)
