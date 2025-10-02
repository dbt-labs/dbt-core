from typing import Dict

import agate
import pytest

from dbt.artifacts.resources import FunctionReturns
from dbt.artifacts.resources.types import FunctionType
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
        data_type: float
        description: A number to be doubled
    returns:
      data_type: float
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
        assert function_node.type == FunctionType.Scalar
        assert function_node.description == "Doubles whatever number is passed in"
        assert len(function_node.arguments) == 1
        argument = function_node.arguments[0]
        assert argument.name == "value"
        assert argument.data_type == "float"
        assert argument.description == "A number to be doubled"
        assert function_node.returns == FunctionReturns(data_type="float")


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
        assert argument.data_type == "float"
        assert results[0].node.returns == FunctionReturns(data_type="float")


class TestCanInlineShowUDF(BasicUDFSetup):
    def test_can_inline_show_udf(self, project):
        run_dbt(["build"])

        result = run_dbt(["show", "--inline", "select {{ function('double_it') }}(1)"])
        assert len(result.results) == 1
        agate_table = result.results[0].agate_table
        assert isinstance(agate_table, agate.Table)
        assert agate_table.column_names == ("double_it",)
        assert agate_table.rows == [(2.0,)]


class TestCanCallUDFInModel(BasicUDFSetup):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "double_it_model.sql": "select {{ function('double_it') }}(1) as double_it",
        }

    def test_can_call_udf_in_model(self, project):
        run_dbt(["build"])

        result = run_dbt(["show", "--select", "double_it_model"])
        assert len(result.results) == 1
        agate_table = result.results[0].agate_table
        assert isinstance(agate_table, agate.Table)
        assert agate_table.column_names == ("double_it",)
        assert agate_table.rows == [(2.0,)]


class TestCanUseWithEmptyMode(BasicUDFSetup):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "double_it_model.sql": "select {{ function('double_it') }}(1) as double_it",
        }

    def test_can_use_with_empty_model(self, project):
        run_dbt(["build", "--empty"])

        result = run_dbt(["show", "--select", "double_it_model"])
        assert len(result.results) == 1
        agate_table = result.results[0].agate_table
        assert isinstance(agate_table, agate.Table)
        assert agate_table.column_names == ("double_it",)
        assert agate_table.rows == [(2.0,)]
