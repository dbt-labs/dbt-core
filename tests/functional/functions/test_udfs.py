from typing import Dict

import pytest

from dbt.artifacts.resources import FunctionReturnType
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt

double_it_sql = """
SELECT value * 2
"""

double_it_yml = """
functions:
  - name: double_it
    description: Doubles whatever number is passed in
    arguments:
      - name: value
        type: float
        description: A number to be doubled
    return_type:
      type: float
"""


class BasicUDFSetup:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_it.sql": double_it_sql,
            "double_it.yml": double_it_yml,
        }


class TestBasicSQLUDF(BasicUDFSetup):
    def test_basic_sql_udf_parsing(self, project):
        manifest = run_dbt(["parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert isinstance(function_node, FunctionNode)
        assert function_node.description == "Doubles whatever number is passed in"
        assert len(function_node.arguments) == 1
        argument = function_node.arguments[0]
        assert argument.name == "value"
        assert argument.type == "float"
        assert argument.description == "A number to be doubled"
        assert function_node.return_type == FunctionReturnType(type="float")


class TestCreationOfUDFs(BasicUDFSetup):
    def test_can_create_udf(self, project):
        results = run_dbt(["build"])
        assert len(results) == 1

        function_node = results[0].node
        assert isinstance(function_node, FunctionNode)
        assert function_node.name == "double_it"
        assert function_node.description == "Doubles whatever number is passed in"

        argument = function_node.arguments[0]
        assert argument.name == "value"
        assert argument.type == "float"
        assert results[0].node.return_type == FunctionReturnType(type="float")
