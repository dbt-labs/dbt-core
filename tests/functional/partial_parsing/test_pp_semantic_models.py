import pytest

from dbt.tests.util import write_file
from tests.functional.assertions.test_runner import dbtTestRunner

# A minimal v2 inline schema with both a simple metric and a non-simple (cumulative) metric.
# The non-simple metric is stored in schema_file.metrics (generated_from=None), while the
# simple metric is stored in schema_file.metrics_from_measures (generated_from=sm_name).
_schema_v1 = """
models:
  - name: fct_revenue
    description: Revenue model v1
    semantic_model: true
    agg_time_dimension: ds
    columns:
      - name: id
        entity:
          type: primary
          name: id_entity
      - name: revenue_date
        granularity: day
        dimension:
          name: ds
          type: time
    metrics:
      - name: simple_revenue
        label: Simple Revenue
        type: simple
        agg: count
        expr: id
      - name: cumulative_revenue
        label: Cumulative Revenue
        type: cumulative
        grain_to_date: day
        period_agg: first
        input_metric: simple_revenue
"""

# Same structure, description changed — triggers partial parsing of the model entry.
_schema_v2 = """
models:
  - name: fct_revenue
    description: Revenue model v2
    semantic_model: true
    agg_time_dimension: ds
    columns:
      - name: id
        entity:
          type: primary
          name: id_entity
      - name: revenue_date
        granularity: day
        dimension:
          name: ds
          type: time
    metrics:
      - name: simple_revenue
        label: Simple Revenue
        type: simple
        agg: count
        expr: id
      - name: cumulative_revenue
        label: Cumulative Revenue
        type: cumulative
        grain_to_date: day
        period_agg: first
        input_metric: simple_revenue
"""

_fct_revenue_sql = "select 1 as id, current_date as revenue_date"
_metricflow_time_spine_sql = "select current_date as date_day"


class TestV2InlineMetricsPartialParsing:
    """
    Regression test for https://github.com/dbt-labs/dbt-core/issues/13004.

    Partial parsing must not raise a duplicate-metric CompilationError after an
    inline v2 model entry is modified.  Non-simple metrics (cumulative, derived,
    ratio, conversion) are tracked in schema_file.metrics rather than
    schema_file.metrics_from_measures; before the fix, _delete_v2_semantic_model_for_model
    only cleaned up metrics_from_measures, leaving stale unique_ids in schema_file.metrics
    and saved_manifest.metrics, which caused add_metric to raise a duplicate error on
    the next parse.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _schema_v1,
            "fct_revenue.sql": _fct_revenue_sql,
            "metricflow_time_spine.sql": _metricflow_time_spine_sql,
        }

    def test_no_duplicate_metrics_after_model_change(self, project):
        runner = dbtTestRunner()

        result = runner.invoke(["parse"])
        assert result.success, result.exception
        assert len(result.result.metrics) == 2
        assert len(result.result.semantic_models) == 1

        # Modify the model entry — triggers partial parsing via _delete_v2_semantic_model_for_model
        write_file(_schema_v2, project.project_root, "models", "schema.yml")

        result = runner.invoke(["parse"])
        # Before the fix this raised CompilationError: "Duplicate metric..."
        assert result.success, result.exception
        assert len(result.result.metrics) == 2
        assert len(result.result.semantic_models) == 1
