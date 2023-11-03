import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.tests.util import update_config_file
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.configs.fixtures import BaseConfigProject
from tests.functional.saved_queries.fixtures import saved_queries_yml, saved_query_description
from tests.functional.semantic_models.fixtures import (
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml,
)


class TestSavedQueryConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "saved-queries": {
                "test": {
                    "test_saved_query": {
                        "+enabled": True,
                    }
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "saved_queries.yml": saved_queries_yml,
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "docs.md": saved_query_description,
        }

    def test_basic_saved_query_config(
        self,
        project,
    ):
        runner = dbtTestRunner()

        # parse with default fixture project config
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)
        assert len(result.result.saved_queries) == 1

        # disable the saved_query via project config and rerun
        config_patch = {"saved-queries": {"test": {"test_saved_query": {"+enabled": False}}}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")
        result = runner.invoke(["parse"])
        assert result.success
        assert len(result.result.saved_queries) == 0
