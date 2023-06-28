import pytest

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.tests.util import write_file


schema_yml = """models:
  - name: fct_revenue
    description: This is the model fct_revenue. It should be able to use doc blocks

semantic_models:
  - name: revenue
    description: This is the revenue semantic model. It should be able to use doc blocks
    model: ref('fct_revenue')

    defaults:
      agg_time_dimension: ds

    measures:
      - name: txn_revenue
        expr: revenue
        agg: sum
      - name: sum_of_things
        expr: 2
        agg: sum
      - name: has_revenue
        expr: true
        agg: sum_boolean
      - name: discrete_order_value_p99
        expr: order_total
        agg: percentile
        agg_params:
          percentile: 0.99
          use_discrete_percentile: True
          use_approximate_percentile: False
      - name: test_agg_params_optional_are_empty
        expr: order_total
        agg: percentile
        agg_params:
          percentile: 0.99

    dimensions:
      - name: ds
        type: time
        expr: created_at
        type_params:
          time_granularity: day

    entities:
      - name: user
        type: foreign
        expr: user_id
"""

schema_without_semantic_model_yml = """models:
  - name: fct_revenue
    description: This is the model fct_revenue. It should be able to use doc blocks
"""

fct_revenue_sql = """select
  1 as id,
  10 as user_id,
  1000 as revenue,
  current_timestamp as created_at"""


class TestSemanticModelParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
        }

    def test_semantic_model_parsing(self, project):
        runner = dbtRunner()
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
        assert len(semantic_model.measures) == 5

    def test_semantic_model_changed_partial_parsing(self, project):
        # First, use the default schema.yml to define our semantic model, and
        # run the dbt parse command
        runner = dbtRunner()
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
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert "semantic_model.test.revenue" in result.result.semantic_models

        # Next, modify the default schema.yml to remove the semantic model.
        write_file(schema_without_semantic_model_yml, project.project_root, "models", "schema.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Finally, verify that the manifest reflects the deletion
        assert "semanticmodel.test.revenue" not in result.result.semantic_models
