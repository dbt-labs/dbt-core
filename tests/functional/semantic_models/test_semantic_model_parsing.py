from typing import List

import pytest

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from dbt.contracts.graph.manifest import Manifest
from dbt.events.base_types import BaseEvent
from dbt.tests.util import write_file
from tests.functional.assertions.test_runner import dbtTestRunner

from tests.functional.semantic_models.fixtures import (
    schema_without_semantic_model_yml,
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml,
)


class TestSemanticModelParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
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
        semantic_model = manifest.semantic_models["semantic_model.test.revenue"]
        assert semantic_model.node_relation.alias == "fct_revenue"
        assert (
            semantic_model.node_relation.relation_name
            == f'"dbt"."{project.test_schema}"."fct_revenue"'
        )
        assert len(semantic_model.measures) == 6
        # manifest should have one metric (that was created from a measure)
        assert len(manifest.metrics) == 2
        metric = manifest.metrics["metric.test.txn_revenue"]
        assert metric.name == "txn_revenue"

    def test_semantic_model_error(self, project):
        # Next, modify the default schema.yml to remove the semantic model.
        error_schema_yml = schema_yml.replace("sum_of_things", "has_revenue")
        write_file(error_schema_yml, project.project_root, "models", "schema.yml")
        events: List[BaseEvent] = []
        runner = dbtTestRunner(callbacks=[events.append])
        result = runner.invoke(["parse"])
        assert not result.success

        validation_errors = [e for e in events if e.info.name == "SemanticValidationFailure"]
        assert validation_errors


class TestSemanticModelPartialParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_semantic_model_changed_partial_parsing(self, project):
        # First, use the default schema.yml to define our semantic model, and
        # run the dbt parse command
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success

        # Next, modify the default schema.yml to change a detail of the semantic
        # model.
        modified_schema_yml = schema_yml.replace("time_granularity: day", "time_granularity: week")
        write_file(modified_schema_yml, project.project_root, "models", "schema.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Finally, verify that the manifest reflects the partially parsed change
        manifest = result.result
        semantic_model = manifest.semantic_models["semantic_model.test.revenue"]
        assert semantic_model.dimensions[0].type_params.time_granularity == TimeGranularity.WEEK

    def test_semantic_model_deleted_partial_parsing(self, project):
        # First, use the default schema.yml to define our semantic model, and
        # run the dbt parse command
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert "semantic_model.test.revenue" in result.result.semantic_models

        # Next, modify the default schema.yml to remove the semantic model.
        write_file(schema_without_semantic_model_yml, project.project_root, "models", "schema.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Finally, verify that the manifest reflects the deletion
        assert "semantic_model.test.revenue" not in result.result.semantic_models

    def test_semantic_model_flipping_create_metric_partial_parsing(self, project):
        generated_metric = "metric.test.txn_revenue"
        # First, use the default schema.yml to define our semantic model, and
        # run the dbt parse command
        write_file(schema_yml, project.project_root, "models", "schema.yml")
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success

        # Verify the metric created by `create_metric: true` exists
        metric = result.result.metrics[generated_metric]
        assert metric.name == "txn_revenue"

        # --- Next, modify the default schema.yml to have no `create_metric: true` ---
        no_create_metric_schema_yml = schema_yml.replace(
            "create_metric: true", "create_metric: false"
        )
        write_file(no_create_metric_schema_yml, project.project_root, "models", "schema.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Verify the metric originally created by `create_metric: true` was removed
        assert result.result.metrics.get(generated_metric) is None

        # Verify that partial parsing didn't clobber the normal metric
        assert result.result.metrics.get("metric.test.simple_metric") is not None

        # --- Now bring it back ---
        create_metric_schema_yml = schema_yml.replace(
            "create_metric: false", "create_metric: true"
        )
        write_file(create_metric_schema_yml, project.project_root, "models", "schema.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Verify the metric originally created by `create_metric: true` was removed
        metric = result.result.metrics[generated_metric]
        assert metric.name == "txn_revenue"
