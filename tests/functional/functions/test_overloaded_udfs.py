"""Functional tests for overloaded UDF support via overloads (dbt-labs/dbt-core#12250).

Overloaded UDFs are defined as a root function with an `overloads` block in YAML.
Each overload references a separate SQL file (via `defined_in`) with different
argument signatures. They appear as a single node in the DAG.
"""

from typing import Dict

import pytest

from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt, write_file

# -- Fixtures: root function + one overload ------------------------------------

double_int_sql = """
SELECT val * 2
"""

double_float_sql = """
SELECT val * 2.0
"""

overloaded_functions_yml = """
functions:
  - name: double_int
    description: "Doubles a value (integer and float overloads)"
    arguments:
      - name: val
        data_type: integer
    returns:
      data_type: integer
    overloads:
      - defined_in: double_float
        arguments:
          - name: val
            data_type: float
        returns:
          data_type: float
"""

model_using_overloaded_function_sql = """
SELECT {{ function('double_int') }}(5) as result
"""


class TestOverloadedUDFParsing:
    """Parsing a root function with overloads."""

    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_int.sql": double_int_sql,
            "double_float.sql": double_float_sql,
            "schema.yml": overloaded_functions_yml,
        }

    def test_overloads_parsed_as_single_node(self, project):
        manifest = run_dbt(["parse"])

        # Only the root function should exist — overload is absorbed
        assert len(manifest.functions) == 1
        assert "function.test.double_int" in manifest.functions
        assert "function.test.double_float" not in manifest.functions

        fn = manifest.functions["function.test.double_int"]
        assert isinstance(fn, FunctionNode)

        # Root has its own arguments
        assert fn.arguments[0].data_type == "integer"

        # And one overload with different arguments
        assert len(fn.overloads) == 1
        overload = fn.overloads[0]
        assert overload.defined_in == "double_float"
        assert overload.arguments[0].data_type == "float"
        assert overload.body is not None
        assert "2.0" in overload.body


class TestOverloadedUDFDependency:
    """Model depends on one node, not multiple."""

    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_int.sql": double_int_sql,
            "double_float.sql": double_float_sql,
            "schema.yml": overloaded_functions_yml,
        }

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "my_model.sql": model_using_overloaded_function_sql,
        }

    def test_model_depends_on_single_function_node(self, project):
        manifest = run_dbt(["parse"])

        model = manifest.nodes["model.test.my_model"]
        fn_deps = [d for d in model.depends_on.nodes if d.startswith("function.")]
        assert fn_deps == ["function.test.double_int"]


class TestOverloadedUDFBuild:
    """Building creates root + overload functions in one node execution."""

    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_int.sql": double_int_sql,
            "double_float.sql": double_float_sql,
            "schema.yml": overloaded_functions_yml,
        }

    def test_build_creates_root_and_overloads(self, project):
        results = run_dbt(["build"])
        # Only 1 function node in the DAG
        assert len(results) == 1

        fn_node = results[0].node
        assert isinstance(fn_node, FunctionNode)
        assert fn_node.name == "double_int"


updated_double_float_sql = """
SELECT val * 3.0
"""


class TestOverloadedUDFPartialParsing:
    """Partial parsing picks up changes to overload SQL files."""

    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_int.sql": double_int_sql,
            "double_float.sql": double_float_sql,
            "schema.yml": overloaded_functions_yml,
        }

    def test_overload_file_change_updates_root(self, project):
        # Initial parse
        manifest = run_dbt(["parse"])
        fn = manifest.functions["function.test.double_int"]
        assert "2.0" in fn.overloads[0].body

        # Change the overload SQL file
        write_file(updated_double_float_sql, project.project_root, "functions", "double_float.sql")

        # Partial parse should pick up the change
        manifest = run_dbt(["parse"])
        fn = manifest.functions["function.test.double_int"]
        assert len(fn.overloads) == 1
        assert "3.0" in fn.overloads[0].body
