import pytest

from dbt.contracts.graph.manifest import Manifest
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.semantic_models.fixtures import (
    base_schema_yml_v2,
    fct_revenue_sql,
    metricflow_time_spine_sql,
)


class TestSemanticModelParsingWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_semantic_model_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = result.result
        assert len(manifest.semantic_models) == 1
        # TODO: Add support for renaming semantic model to be different than the dbt model
        # semantic_model = manifest.semantic_models["semantic_model.test.revenue"]
        semantic_model = manifest.semantic_models["semantic_model.test.fct_revenue"]
        assert semantic_model.node_relation.alias == "fct_revenue"
        assert (
            semantic_model.node_relation.relation_name
            == f'"dbt"."{project.test_schema}"."fct_revenue"'
        )
        assert (
            semantic_model.description
            == "This is the model fct_revenue. It should be able to use doc blocks"
        )
        # No measures in v2 YAML
        assert len(semantic_model.measures) == 0
        # TODO: Metrics are not parsed yet
        assert len(manifest.metrics) == 0
        # TODO: Dimensions are not parsed yet (for those attached to model columns)
        # TODO: Dimensions are not parsed yet (for those defined in derived semantics)
