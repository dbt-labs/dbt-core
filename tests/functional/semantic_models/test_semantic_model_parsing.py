from typing import List

import pytest

from dbt.artifacts.resources.v1.semantic_model import MetricType
from dbt.contracts.graph.manifest import Manifest
from dbt.tests.util import run_dbt, write_file
from dbt_common.events.base_types import BaseEvent
from dbt_semantic_interfaces.type_enums.conversion_calculation_type import (
    ConversionCalculationType,
)
from dbt_semantic_interfaces.type_enums.period_agg import PeriodAggregation
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.semantic_models.fixtures import (
    base_schema_yml,
    conversion_metric_yml,
    fct_revenue_sql,
    metricflow_time_spine_sql,
    multi_sm_schema_yml,
    ratio_metric_yml,
    schema_without_semantic_model_yml,
    schema_yml,
)


class TestSemanticModelParsingWorks:
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
        assert len(semantic_model.measures) == 9
        # manifest should have two metrics created from measures
        assert len(manifest.metrics) == 7
        metric = manifest.metrics["metric.test.txn_revenue"]
        assert metric.name == "txn_revenue"
        metric_with_label = manifest.metrics["metric.test.txn_revenue_with_label"]
        assert metric_with_label.name == "txn_revenue_with_label"
        assert metric_with_label.label == "Transaction Revenue with label"


class TestSemanticModelParsingErrors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

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


class TestSemanticModelParsingForCumulativeMetrics:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_cumulative_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        assert "metric.test.test_cumulative_metric" in manifest.metrics
        metric = manifest.metrics["metric.test.test_cumulative_metric"]
        assert metric.name == "test_cumulative_metric"
        # Check type and params for cumulative metric
        assert metric.type is MetricType.CUMULATIVE
        cumulative_params = metric.type_params.cumulative_type_params
        assert cumulative_params is not None
        assert cumulative_params.grain_to_date == "day"
        assert cumulative_params.period_agg is PeriodAggregation.FIRST

        assert len(metric.type_params.input_measures) == 1
        assert "sum_of_things" in [im.name for im in metric.type_params.input_measures]

        # Not sure where dbt uses this, but for now, let's just make sure we know
        # we're keeping the contract that these metrics are marked as depending on
        # the semantic model.
        assert "semantic_model.test.revenue" in metric.depends_on.nodes


class TestSemanticModelParsingForConversionMetrics:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_conversion_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        assert "metric.test.test_conversion_metric" in manifest.metrics
        metric = manifest.metrics["metric.test.test_conversion_metric"]
        assert metric.name == "test_conversion_metric"
        # Check type and params for conversion metric
        assert metric.type is MetricType.CONVERSION
        conversion_params = metric.type_params.conversion_type_params
        assert conversion_params is not None
        # Confirm measures, entity, and calculation are correct
        assert conversion_params.base_measure.name == "count_of_things"
        assert conversion_params.conversion_measure.name == "count_of_things_2"
        assert conversion_params.entity == "user"
        assert conversion_params.calculation == ConversionCalculationType.CONVERSION_RATE

        assert len(metric.type_params.input_measures) == 2
        assert "count_of_things" in [im.name for im in metric.type_params.input_measures]
        assert "count_of_things_2" in [im.name for im in metric.type_params.input_measures]

        # Not sure where dbt uses this, but for now, let's just make sure
        # we're keeping the contract that these metrics are marked as depending on
        # the semantic model.
        assert "semantic_model.test.revenue" in metric.depends_on.nodes


class TestSemanticModelParsingForRatioMetrics:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_ratio_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        assert "metric.test.test_ratio_metric" in manifest.metrics
        metric = manifest.metrics["metric.test.test_ratio_metric"]
        assert metric.name == "test_ratio_metric"
        assert metric.type is MetricType.RATIO
        type_params = metric.type_params
        assert type_params.numerator.name == "simple_metric"
        assert type_params.denominator.name == "test_conversion_metric"

        assert len(metric.type_params.input_measures) == 3
        assert "sum_of_things" in [im.name for im in metric.type_params.input_measures]
        assert "count_of_things" in [im.name for im in metric.type_params.input_measures]
        assert "count_of_things_2" in [im.name for im in metric.type_params.input_measures]

        # I don't know for sure why, but it seems like we've never marked the
        # 'depends_on' semantic model for ratio metrics, so let's document that here
        # as a test.  These are not used in metricflow.
        assert len(metric.depends_on.nodes) == 2
        assert "metric.test.simple_metric" in metric.depends_on.nodes
        assert "metric.test.test_conversion_metric" in metric.depends_on.nodes


class TestSemanticModelParsingForDerivedMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_derived_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        assert "metric.test.test_derived_metric" in manifest.metrics
        metric = manifest.metrics["metric.test.test_derived_metric"]
        assert metric.name == "test_derived_metric"
        assert metric.type is MetricType.DERIVED
        assert len(metric.type_params.input_measures) == 3
        assert "sum_of_things" in [im.name for im in metric.type_params.input_measures]
        assert "count_of_things" in [im.name for im in metric.type_params.input_measures]
        assert "count_of_things_2" in [im.name for im in metric.type_params.input_measures]

        assert len(metric.type_params.metrics) == 2
        assert "simple_metric" in [m.name for m in metric.type_params.metrics]
        assert "test_conversion_metric" in [m.name for m in metric.type_params.metrics]

        # I don't know for sure why, but it seems like we've never marked the
        # 'depends_on' semantic model for derived metrics, so let's document that here
        # as a test.  These are not used in metricflow.
        assert len(metric.depends_on.nodes) == 2
        assert "metric.test.simple_metric" in metric.depends_on.nodes
        assert "metric.test.test_conversion_metric" in metric.depends_on.nodes


# ------------------------------------------------------------------------------
# Partial Parsing tests below
# ------------------------------------------------------------------------------


class TestSemanticModelPartialParsingWithModelChanged:
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


# TODO DI-4421 (Linear) / SEMANTIC-2997 (Jira): fix the partial parsing bug that breaks this.
# class TestSemanticModelPartialParsingWithModelDeleted:
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "schema.yml": schema_yml,
#             "fct_revenue.sql": fct_revenue_sql,
#             "metricflow_time_spine.sql": metricflow_time_spine_sql,
#         }

#     def test_semantic_model_deleted_partial_parsing__dependency_bug_causes_failure(self, project):
#         # First, use the default schema.yml to define our semantic model, and
#         # run the dbt parse command
#         runner = dbtTestRunner()
#         result = runner.invoke(["parse"])
#         assert result.success
#         assert "semantic_model.test.revenue" in result.result.semantic_models

#         # Next, modify the default schema.yml to remove the semantic model.
#         write_file(schema_without_semantic_model_yml, project.project_root, "models", "schema.yml")

#         # Now, run the dbt parse command again.
#         result = runner.invoke(["parse"])
#         # Known bug: we don't remove metrics in any particular order, so we'll remove
#         # simple_metric before the metrics that rely on it and have problems... but only SOMETIMES.
#         assert result.success


class TestSemanticModelPartialParsingWithModelDeletedIteratively:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml + conversion_metric_yml + ratio_metric_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_semantic_model_deleted_partial_parsing(self, project):
        # First, use the default schema.yml to define our semantic model, and
        # run the dbt parse command
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert "semantic_model.test.revenue" in result.result.semantic_models

        # Next, modify the default schema.yml to remove the semantic model ITERATIVELY
        # (removing it all at once doesn't work because of a bug right now.
        # see test_semantic_model_deleted_partial_parsing__dependency_bug_causes_failure.)
        write_file(
            base_schema_yml + conversion_metric_yml,
            project.project_root,
            "models",
            "schema.yml",
        )
        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        write_file(base_schema_yml, project.project_root, "models", "schema.yml")
        result = runner.invoke(["parse"])
        assert result.success

        write_file(schema_without_semantic_model_yml, project.project_root, "models", "schema.yml")
        result = runner.invoke(["parse"])
        assert result.success

        # Finally, verify that the manifest reflects the deletion
        assert "semantic_model.test.revenue" not in result.result.semantic_models


class TestSemanticModelPartialParsingWithModelFlippingCreateMetric:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_semantic_model_flipping_create_metric_partial_parsing(self, project):
        generated_metric = "metric.test.txn_revenue"
        generated_metric_with_label = "metric.test.txn_revenue_with_label"
        # First, use the default schema.yml to define our semantic model, and
        # run the dbt parse command
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success

        # Verify the metric created by `create_metric: true` exists
        metric = result.result.metrics[generated_metric]
        assert metric.name == "txn_revenue"
        assert metric.label == "txn_revenue"

        metric_with_label = result.result.metrics[generated_metric_with_label]
        assert metric_with_label.name == "txn_revenue_with_label"
        assert metric_with_label.label == "Transaction Revenue with label"

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


class TestSemanticModelPartialParsingGeneratedMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": multi_sm_schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_generated_metrics(self, project):
        manifest = run_dbt(["parse"])
        expected = {
            "metric.test.simple_metric",
            "metric.test.txn_revenue",
            "metric.test.alt_txn_revenue",
        }
        assert set(manifest.metrics.keys()) == expected

        # change description of 'revenue' semantic model
        modified_schema_yml = multi_sm_schema_yml.replace("first", "FIRST")
        write_file(modified_schema_yml, project.project_root, "models", "schema.yml")
        manifest = run_dbt(["parse"])
        assert set(manifest.metrics.keys()) == expected
