import pytest

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

schema_yml = """models:
  - name: fct_revenue
    description: This is the model fct_revenue. It should be able to use doc blocks

semantic_models:
  - name: revenue
    description: This is the revenue semantic model. It should be able to use doc blocks
    model: ref('fct_revenue')

    measures:
      - name: txn_revenue
        expr: revenue
        agg: sum

    dimensions:
      - name: ds
        type: time
        expr: created_at
        type_params:
          is_primary: True
          time_granularity: day

    entities:
      - name: user
        type: foreign
        expr: user_id
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
        assert len(manifest.semantic_nodes) == 1
        semantic_model = manifest.semantic_nodes["semanticmodel.test.revenue"]
        assert semantic_model.node_relation.alias == "fct_revenue"
