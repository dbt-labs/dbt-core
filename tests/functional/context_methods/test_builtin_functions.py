import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
from dbt.exceptions import CompilationException

macros__validate_set_sql = """
{% macro validate_set() %}
    {% set set_result = set([1, 2, 2, 3, 'foo', False]) %}
    {{ log("set_result: " ~ set_result) }}
    {% set set_strict_result = set_strict([1, 2, 2, 3, 'foo', False]) %}
    {{ log("set_strict_result: " ~ set_strict_result) }}
{% endmacro %}
"""

macros__validate_zip_sql = """
{% macro validate_zip() %}
    {% set list_a = [1, 2] %}
    {% set list_b = ['foo', 'bar'] %}
    {% set zip_result = zip(list_a, list_b) | list %}
    {{ log("zip_result: " ~ zip_result) }}
    {% set zip_strict_result = zip_strict(list_a, list_b) | list %}
    {{ log("zip_strict_result: " ~ zip_strict_result) }}
{% endmacro %}
"""

models__set_exception_sql = """
{% set set_strict_result = set_strict(1) %}
"""

models__zip_exception_sql = """
{% set zip_strict_result = zip_strict(1) %}
"""


class TestContextBuiltins:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_set.sql": macros__validate_set_sql,
            "validate_zip.sql": macros__validate_zip_sql,
        }

    def test_builtin_set_function(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "run-operation", "validate_set"])

        # The order of the set isn't guaranteed so we can't check for the actual set in the logs
        assert "set_result: " in log_output
        assert "False" in log_output
        assert "set_strict_result: " in log_output

    def test_builtin_zip_function(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "run-operation", "validate_zip"])

        expected_zip = [(1, "foo"), (2, "bar")]
        assert f"zip_result: {expected_zip}" in log_output
        assert f"zip_strict_result: {expected_zip}" in log_output


class TestContextBuiltinExceptions:
    # Assert compilation errors are raised with _strict equivalents
    def test_builtin_function_exception(self, project):
        write_file(models__set_exception_sql, project.project_root, "models", "raise.sql")
        with pytest.raises(CompilationException):
            run_dbt(["compile"])

        write_file(models__zip_exception_sql, project.project_root, "models", "raise.sql")
        with pytest.raises(CompilationException):
            run_dbt(["compile"])
