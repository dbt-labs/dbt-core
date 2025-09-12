from typing import Dict

import pytest

from dbt.artifacts.resources import FunctionReturnType
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt

area_of_circle_sql = """
SELECT pi() * radius * radius
"""

area_of_circle_yml = """
functions:
  - name: area_of_circle
    description: Calculates the area of a circle for a given radius
    arguments:
      - name: radius
        type: float
        description: A floating point number representing the radius of the circle
    return_type:
      type: float
"""


class BasicUDFSetup:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "area_of_circle.sql": area_of_circle_sql,
            "area_of_circle.yml": area_of_circle_yml,
        }


class TestBasicSQLUDF(BasicUDFSetup):
    def test_basic_sql_udf_parsing(self, project):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 1
        assert "function.test.area_of_circle" in manifest.nodes
        function_node = manifest.nodes["function.test.area_of_circle"]
        assert isinstance(function_node, FunctionNode)
        assert function_node.description == "Calculates the area of a circle for a given radius"
        assert len(function_node.arguments) == 1
        argument = function_node.arguments[0]
        assert argument.name == "radius"
        assert argument.type == "float"
        assert (
            argument.description == "A floating point number representing the radius of the circle"
        )
        assert function_node.return_type == FunctionReturnType(type="float")


class TestCreationOfUDFs(BasicUDFSetup):
    def test_can_create_udf(self, project):
        results = run_dbt(["build"])
        assert len(results) == 1

        function_node = results[0].node
        assert isinstance(function_node, FunctionNode)
        assert function_node.name == "area_of_circle"
        assert function_node.description == "Calculates the area of a circle for a given radius"

        argument = function_node.arguments[0]
        assert argument.name == "radius"
        assert argument.type == "float"
        assert results[0].node.return_type == FunctionReturnType(type="float")
