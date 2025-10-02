from typing import Dict

import pytest

from dbt.artifacts.resources import FunctionReturns
from dbt.artifacts.resources.types import FunctionType
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt

double_total_sql = """
SELECT SUM(values) * 2
"""

double_total_yml = """
functions:
  - name: double_total
    type: aggregate
    description: Sums the sequence of numbers and then doubles the result
    arguments:
      - name: values
        data_type: float
        description: A sequence of numbers
    returns:
      data_type: float
"""


class BasicUDAFSetup:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_total.sql": double_total_sql,
            "double_total.yml": double_total_yml,
        }


class TestBasicSQLUDAF(BasicUDAFSetup):
    def test_basic_sql_udaf_parsing(self, project):
        manifest = run_dbt(["parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_total" in manifest.functions
        function_node = manifest.functions["function.test.double_total"]
        assert isinstance(function_node, FunctionNode)
        assert function_node.type == FunctionType.Aggregate
        assert (
            function_node.description == "Sums the sequence of numbers and then doubles the result"
        )
        assert len(function_node.arguments) == 1
        argument = function_node.arguments[0]
        assert argument.name == "values"
        assert argument.data_type == "float"
        assert argument.description == "A sequence of numbers"
        assert function_node.returns == FunctionReturns(data_type="float")
