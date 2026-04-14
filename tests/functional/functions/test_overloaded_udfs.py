"""Functional tests for overloaded UDF support (dbt-labs/dbt-core#12250).

Overloaded UDFs allow multiple functions to share the same database name
(alias) while having different argument signatures. Each overload lives in
its own SQL file with a unique filename but declares the same alias.
"""

from typing import Dict

import pytest

from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt

# -- Fixtures: two SQL overloads with different argument types ----------------

double_int_sql = """
SELECT val * 2
"""

double_float_sql = """
SELECT val * 2.0
"""

overloaded_functions_yml = """
functions:
  - name: double_int
    description: "Doubles an integer value"
    config:
      alias: double_val
    arguments:
      - name: val
        data_type: integer
    returns:
      data_type: integer
  - name: double_float
    description: "Doubles a float value"
    config:
      alias: double_val
    arguments:
      - name: val
        data_type: float
    returns:
      data_type: float
"""

model_using_overloaded_function_sql = """
SELECT {{ function('double_val') }}(1) as result
"""


class TestOverloadedUDFParsing:
    """Parsing two functions with different names but the same alias."""

    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_int.sql": double_int_sql,
            "double_float.sql": double_float_sql,
            "schema.yml": overloaded_functions_yml,
        }

    def test_overloaded_functions_parsed_separately(self, project):
        manifest = run_dbt(["parse"])

        # Both overloads should exist as separate function nodes
        assert len(manifest.functions) == 2
        assert "function.test.double_int" in manifest.functions
        assert "function.test.double_float" in manifest.functions

        # Each should have its own arguments and alias
        fn_int = manifest.functions["function.test.double_int"]
        fn_float = manifest.functions["function.test.double_float"]

        assert isinstance(fn_int, FunctionNode)
        assert isinstance(fn_float, FunctionNode)

        # Both share the same alias (database function name)
        assert fn_int.alias == "double_val"
        assert fn_float.alias == "double_val"

        # But have different internal names
        assert fn_int.name == "double_int"
        assert fn_float.name == "double_float"

        # And different argument types
        assert fn_int.arguments[0].data_type == "integer"
        assert fn_float.arguments[0].data_type == "float"


class TestOverloadedUDFLookupByAlias:
    """The function() Jinja method should resolve overloads by alias."""

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
            "model_using_overloaded_function.sql": model_using_overloaded_function_sql,
        }

    def test_model_depends_on_all_overloads(self, project):
        manifest = run_dbt(["parse"])

        # The model should depend on both overloads
        model = manifest.nodes["model.test.model_using_overloaded_function"]
        fn_deps = [dep for dep in model.depends_on.nodes if dep.startswith("function.")]
        assert len(fn_deps) == 2
        assert set(fn_deps) == {
            "function.test.double_int",
            "function.test.double_float",
        }


class TestOverloadedUDFBuild:
    """Building overloaded functions should create all overloads."""

    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_int.sql": double_int_sql,
            "double_float.sql": double_float_sql,
            "schema.yml": overloaded_functions_yml,
        }

    def test_build_creates_all_overloads(self, project):
        results = run_dbt(["build"])
        assert len(results) == 2

        # Both overloads should be built successfully
        function_nodes = [r.node for r in results]
        assert all(isinstance(n, FunctionNode) for n in function_nodes)

        names = {n.name for n in function_nodes}
        assert names == {"double_int", "double_float"}

        # Both should use the same alias for the database function name
        aliases = {n.alias for n in function_nodes}
        assert aliases == {"double_val"}
