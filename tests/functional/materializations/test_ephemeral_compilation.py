from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.results import RunExecutionResult, RunResult
import pytest
from dbt.tests.util import run_dbt

# Note: This tests compilation only, so is a dbt Core test and not an adapter test.
# There is some complicated logic in core/dbt/compilation.py having to do with
# ephemeral nodes and handling multiple threads at the same time. This test
# fails fairly regularly if that is broken, but does occasionally work (depending
# on the order in which things are compiled). It requires multi-threading to fail.


fct_eph_first_sql = """
-- fct_eph_first.sql
{{ config(materialized='ephemeral') }}

with int_eph_first as(
    select * from {{ ref('int_eph_first') }}
)

select * from int_eph_first
"""

int_eph_first_sql = """
-- int_eph_first.sql
{{ config(materialized='ephemeral') }}

select
    1 as first_column,
    2 as second_column
"""

schema_yml = """
version: 2

models:
  - name: int_eph_first
    columns:
      - name: first_column
        tests:
          - not_null
      - name: second_column
        tests:
          - not_null

  - name: fct_eph_first
    columns:
      - name: first_column
        tests:
          - not_null
      - name: second_column
        tests:
          - not_null

"""


SUPPRESSED_CTE_EXPECTED_OUTPUT = """-- fct_eph_first.sql


with int_eph_first as(
    select * from __dbt__cte__int_eph_first
)

select * from int_eph_first"""


class TestEphemeralCompilation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "int_eph_first.sql": int_eph_first_sql,
            "fct_eph_first.sql": fct_eph_first_sql,
            "schema.yml": schema_yml,
        }

    def test_ephemeral_compilation(self, project):
        # Note: There are no models that run successfully. This testcase tests running tests.
        results = run_dbt(["run"])
        assert len(results) == 0

    def test__suppress_injected_ctes(self, project):
        compile_output = run_dbt(
            ["compile", "--no-inject-ephemeral-ctes", "--select", "fct_eph_first"]
        )
        assert isinstance(compile_output, RunExecutionResult)
        node_result = compile_output.results[0]
        assert isinstance(node_result, RunResult)
        node = node_result.node
        assert isinstance(node, ModelNode)
        assert node.compiled_code == SUPPRESSED_CTE_EXPECTED_OUTPUT
