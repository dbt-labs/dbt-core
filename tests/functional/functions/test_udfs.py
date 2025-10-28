from typing import Dict

import agate
import pytest

from dbt.artifacts.resources import FunctionReturns
from dbt.artifacts.resources.types import FunctionType, FunctionVolatility
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt, write_file

double_it_sql = """
SELECT value * 2
"""

double_it_py = """
def entry(value):
    return value * 2
"""

double_it_deterministic_sql = """
{{ config(volatility='deterministic') }}
SELECT value * 2
"""

double_it_deterministic_py = """
{{ config(volatility='deterministic') }}
def entry(value):
    return value * 2
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

double_it_python_yml = """
functions:
  - name: double_it
    description: Doubles whatever number is passed in
    config:
        runtime_version: "3.11"
        entry_point: entry
    arguments:
      - name: value
        data_type: float
        description: A number to be doubled
    returns:
      data_type: float
"""

double_it_non_deterministic_yml = """
functions:
  - name: double_it
    description: Doubles whatever number is passed in
    config:
      volatility: non-deterministic
    arguments:
      - name: value
        data_type: float
        description: A number to be doubled
    returns:
      data_type: float
"""

double_it_non_deterministic_python_yml = """
functions:
  - name: double_it
    description: Doubles whatever number is passed in
    config:
      volatility: non-deterministic
      language_version: "3.11"
      entry_point: entry
    arguments:
      - name: value
        data_type: float
        description: A number to be doubled
    returns:
      data_type: float
"""

numbers_model_sql = """
SELECT 1 as number
UNION ALL
SELECT 2 as number
UNION ALL
SELECT 3 as number
"""

sum_numbers_function_sql = """
SELECT sum(number) as sum_numbers FROM {{ ref('numbers_model') }}
"""

sum_numbers_function_yml = """
functions:
  - name: sum_numbers_function
    description: Sums the numbers in the numbers_model
    returns:
      data_type: integer
"""

numbers_seed_csv = """number
1
2
3
"""

numbers_source_yml = """
sources:
  - name: test_source
    schema: "{{ target.schema }}"
    tables:
      - name: numbers_seed
"""

sum_numbers_function_from_source_sql = """
SELECT sum(number) as sum_numbers FROM {{ source('test_source', 'numbers_seed') }}
"""


class BasicUDFSetup:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_it.sql": double_it_sql,
            "double_it.yml": double_it_yml,
        }


class TestBasicSQLUDF(BasicUDFSetup):
    def test_basic_parsing(self, project):
        # Simple parsing
        manifest = run_dbt(["parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert isinstance(function_node, FunctionNode)
        assert function_node.description == "Doubles whatever number is passed in"
        assert function_node.language == "sql"
        assert function_node.config.type == FunctionType.Scalar
        assert function_node.config.volatility is None
        assert len(function_node.arguments) == 1
        argument = function_node.arguments[0]
        assert argument.name == "value"
        assert argument.data_type == "float"
        assert argument.description == "A number to be doubled"
        assert function_node.returns == FunctionReturns(data_type="float")

        # Update with volatility specified in sql
        write_file(double_it_deterministic_sql, project.project_root, "functions", "double_it.sql")
        manifest = run_dbt(["parse", "--no-partial-parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert function_node.config.volatility == FunctionVolatility.Deterministic

        # Update with volatility specified in yml
        write_file(
            double_it_non_deterministic_yml, project.project_root, "functions", "double_it.yml"
        )
        write_file(double_it_sql, project.project_root, "functions", "double_it.sql")
        manifest = run_dbt(["parse", "--no-partial-parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert function_node.config.volatility == FunctionVolatility.NonDeterministic


class TestBasicPythonUDF(BasicUDFSetup):
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_it.py": double_it_py,
            "double_it.yml": double_it_python_yml,
        }

    def test_basic_parsing(self, project):
        # Simple parsing
        manifest = run_dbt(["parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert isinstance(function_node, FunctionNode)
        assert function_node.description == "Doubles whatever number is passed in"
        assert function_node.language == "python"
        assert function_node.config.type == FunctionType.Scalar
        assert function_node.config.volatility is None
        assert function_node.config.runtime_version == "3.11"
        assert function_node.config.entry_point == "entry"
        assert len(function_node.arguments) == 1
        argument = function_node.arguments[0]
        assert argument.name == "value"
        assert argument.data_type == "float"
        assert argument.description == "A number to be doubled"
        assert function_node.returns == FunctionReturns(data_type="float")

        # Update with volatility specified in sql
        write_file(double_it_deterministic_py, project.project_root, "functions", "double_it.py")
        manifest = run_dbt(["parse", "--no-partial-parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert function_node.config.volatility == FunctionVolatility.Deterministic

        # Update with volatility specified in yml
        write_file(
            double_it_non_deterministic_python_yml,
            project.project_root,
            "functions",
            "double_it.yml",
        )
        write_file(double_it_py, project.project_root, "functions", "double_it.py")
        manifest = run_dbt(["parse", "--no-partial-parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert function_node.config.volatility == FunctionVolatility.NonDeterministic


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


class TestCanUseRefInUDF:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "sum_numbers_function.sql": sum_numbers_function_sql,
            "sum_numbers_function.yml": sum_numbers_function_yml,
        }

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "numbers_model.sql": numbers_model_sql,
        }

    def test_can_use_ref_in_udf(self, project):
        run_dbt(["build"])

        result = run_dbt(
            [
                "show",
                "--inline",
                "select {{ function('sum_numbers_function') }}() as summed_numbers",
            ]
        )
        assert len(result.results) == 1
        agate_table = result.results[0].agate_table
        assert isinstance(agate_table, agate.Table)
        assert agate_table.column_names == ("summed_numbers",)
        assert agate_table.rows == [(6,)]


class TestCanUseSourceInUDF:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "sum_numbers_function.sql": sum_numbers_function_from_source_sql,
            "sum_numbers_function.yml": sum_numbers_function_yml,
        }

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "numbers_source.yml": numbers_source_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self) -> Dict[str, str]:
        return {
            "numbers_seed.csv": numbers_seed_csv,
        }

    def test_can_use_ref_in_udf(self, project):
        run_dbt(["seed"])
        run_dbt(["build"])

        result = run_dbt(
            [
                "show",
                "--inline",
                "select {{ function('sum_numbers_function') }}() as summed_numbers",
            ]
        )
        assert len(result.results) == 1
        agate_table = result.results[0].agate_table
        assert isinstance(agate_table, agate.Table)
        assert agate_table.column_names == ("summed_numbers",)
        assert agate_table.rows == [(6,)]


class TestCanConfigFunctionsFromProjectConfig:
    @pytest.fixture(scope="class")
    def functions(self) -> Dict[str, str]:
        return {
            "double_it.sql": double_it_sql,
            "double_it.yml": double_it_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "stable"},
        }

    def test_can_config_functions_from_project_config(self, project):
        manifest = run_dbt(["parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        assert function_node.config.volatility == FunctionVolatility.Stable

        # Update with volatility specified in sql
        write_file(double_it_deterministic_sql, project.project_root, "functions", "double_it.sql")
        manifest = run_dbt(["parse", "--no-partial-parse"])
        assert len(manifest.functions) == 1
        assert "function.test.double_it" in manifest.functions
        function_node = manifest.functions["function.test.double_it"]
        # Volatility from sql should take precedence over the project config
        assert function_node.config.volatility == FunctionVolatility.Deterministic
