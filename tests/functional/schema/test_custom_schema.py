import pytest

from dbt.tests.util import check_relations_equal, run_dbt
from tests.functional.schema.fixtures.macros import (
    _CUSTOM_MACRO,
    _CUSTOM_MACRO_MULTI_SCHEMA,
    _CUSTOM_MACRO_W_CONFIG,
)
from tests.functional.schema.fixtures.sql import (
    _SEED_CSV,
    _TABLE_ONE,
    _TABLE_ONE_DOT_MODEL_NAME,
    _TABLE_ONE_DOT_MODEL_SCHEMA,
    _TABLE_THREE,
    _TABLE_THREE_DOT_MODEL,
    _TABLE_THREE_SCHEMA,
    _TABLE_TWO,
    _TABLE_TWO_DOT_MODEL,
    _TABLE_TWO_DOT_MODEL_NAME,
    _TABLE_TWO_DOT_MODEL_SCHEMA,
    _TABLE_TWO_SCHEMA,
    _VALIDATION_SQL,
)

_CUSTOM_SCHEMA = "dbt_test"


class BaseTestCustomSchema:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_1.sql": _TABLE_ONE,
            "view_2.sql": _TABLE_TWO,
            "table_3.sql": _TABLE_THREE,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"schema": _CUSTOM_SCHEMA}}


class TestCustomSchema(BaseTestCustomSchema):
    def test__postgres_handles__custom_schema_with_no_prefix(self, project, macros):
        project.run_sql(_VALIDATION_SQL)
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        table_results = {r.node.name: r.node.schema for r in results.results}
        assert table_results["view_1"] == f"{project.test_schema}_{_CUSTOM_SCHEMA}"
        assert table_results["view_2"] == f"{project.test_schema}_{_TABLE_TWO_SCHEMA}"
        assert table_results["table_3"] == f"{project.test_schema}_{_TABLE_THREE_SCHEMA}"
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{project.test_schema}_{_CUSTOM_SCHEMA}.view_1"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{project.test_schema}_{_TABLE_TWO_SCHEMA}.view_2"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("agg", f"{project.test_schema}_{_TABLE_THREE_SCHEMA}.table_3"),
        )


class TestCustomSchemaWithCustomMacro(BaseTestCustomSchema):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_macro.sql": _CUSTOM_MACRO,
        }

    def test__postgres_handles__custom_schema_with_custom_macro(self, project, macros):
        project.run_sql(_VALIDATION_SQL)
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        table_results = {r.node.name: r.node.schema for r in results.results}
        assert table_results["view_1"] == f"{_CUSTOM_SCHEMA}_{project.test_schema}_macro"
        assert table_results["view_2"] == f"{_TABLE_TWO_SCHEMA}_{project.test_schema}_macro"
        assert table_results["table_3"] == f"{_TABLE_THREE_SCHEMA}_{project.test_schema}_macro"
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{_CUSTOM_SCHEMA}_{project.test_schema}_macro.view_1"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{_TABLE_TWO_SCHEMA}_{project.test_schema}_macro.view_2"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("agg", f"{_TABLE_THREE_SCHEMA}_{project.test_schema}_macro.table_3"),
        )


class TestCustomSchemaWithPrefix(BaseTestCustomSchema):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_macro.sql": _CUSTOM_MACRO_W_CONFIG,
        }

    def test__postgres__custom_schema_with_prefix(self, project, macros):
        project.run_sql(_VALIDATION_SQL)
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        table_results = {r.node.name: r.node.schema for r in results.results}
        assert table_results["view_1"] == f"{_CUSTOM_SCHEMA}_{project.test_schema}_macro"
        assert table_results["view_2"] == f"{_TABLE_TWO_SCHEMA}_{project.test_schema}_macro"
        assert table_results["table_3"] == f"{_TABLE_THREE_SCHEMA}_{project.test_schema}_macro"
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{_CUSTOM_SCHEMA}_{project.test_schema}_macro.view_1"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{_TABLE_TWO_SCHEMA}_{project.test_schema}_macro.view_2"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("agg", f"{_TABLE_THREE_SCHEMA}_{project.test_schema}_macro.table_3"),
        )


class TestCustomSchemaWithPrefixAndDispatch(BaseTestCustomSchema):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_macro.sql": _CUSTOM_MACRO_W_CONFIG,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {"schema": _CUSTOM_SCHEMA},
            "dispatch": [
                {
                    "macro_namespace": "dbt",
                    "search_order": ["test", "package_macro_overrides", "dbt"],
                }
            ],
        }

    def test__postgres__custom_schema_with_prefix_and_dispatch(
        self, project, macros, project_config_update
    ):
        project.run_sql(_VALIDATION_SQL)
        run_dbt(["deps"])
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        table_results = {r.node.name: r.node.schema for r in results.results}
        assert table_results["view_1"] == f"{_CUSTOM_SCHEMA}_{project.test_schema}_macro"
        assert table_results["view_2"] == f"{_TABLE_TWO_SCHEMA}_{project.test_schema}_macro"
        assert table_results["table_3"] == f"{_TABLE_THREE_SCHEMA}_{project.test_schema}_macro"
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{_CUSTOM_SCHEMA}_{project.test_schema}_macro.view_1"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("seed", f"{_TABLE_TWO_SCHEMA}_{project.test_schema}_macro.view_2"),
        )
        check_relations_equal(
            adapter=project.adapter,
            relation_names=("agg", f"{_TABLE_THREE_SCHEMA}_{project.test_schema}_macro.table_3"),
        )


class TestCustomSchemaWithCustomMacroFromModelName(BaseTestCustomSchema):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_macro.sql": _CUSTOM_MACRO_MULTI_SCHEMA,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {"schema": _CUSTOM_SCHEMA},
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{_TABLE_ONE_DOT_MODEL_NAME}.sql": _TABLE_ONE,
            f"{_TABLE_TWO_DOT_MODEL_NAME}.sql": _TABLE_TWO_DOT_MODEL,
            "table_3.sql": _TABLE_THREE_DOT_MODEL,
        }

    def test__postgres__custom_schema_from_model_name(
        self, project, macros, project_config_update
    ):
        project.run_sql(_VALIDATION_SQL)
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        table_results = {r.node.name: r.node.schema for r in results.results}

        assert table_results[_TABLE_ONE_DOT_MODEL_NAME] == _TABLE_ONE_DOT_MODEL_SCHEMA
        assert table_results[_TABLE_TWO_DOT_MODEL_NAME] == _TABLE_TWO_DOT_MODEL_SCHEMA
        assert table_results["table_3"] == f"{project.test_schema}"
        check_relations_equal(
            adapter=project.adapter, relation_names=("seed", _TABLE_ONE_DOT_MODEL_NAME)
        )
        check_relations_equal(
            adapter=project.adapter, relation_names=("seed", _TABLE_TWO_DOT_MODEL_NAME)
        )
        check_relations_equal(
            adapter=project.adapter, relation_names=("agg", f"{project.test_schema}.table_3")
        )
