import pytest

from dbt.tests.util import run_dbt, write_file
from dbt.exceptions import CompilationException


tests__set_sql = """
{% set set_result = set([1, 2, 2, 3, "foo", False]) %}
{% set try_set_result = try_set([1, 2, 2, 3, "foo", False]) %}

{% set simple_set = (set_result == try_set_result == set((1, 2, 3, "foo", False))) %}

(select 'simple_set' as name {% if simple_set %}limit 0{% endif %})
"""

tests__zip_sql = """
{% set list_a = [1, 2] %}
{% set list_b = ['foo', 'bar'] %}
{% set zip_result = zip(list_a, list_b) | list %}
{% set try_zip_result = try_zip(list_a, list_b) | list %}

{% set simple_zip = (zip_result == try_zip_result == [(1, 'foo'), (2, 'bar')]) %}

(select 'simple_zip' as name {% if simple_zip %}limit 0{% endif %})
"""

tests__set_exception_sql = """
{% set try_set_result = try_set(1) %}
"""

tests__zip_exception_sql = """
{% set try_set_result = try_zip(1) %}
"""


class TestContextBuiltins:
    # This tests have no actual models
    @pytest.fixture(scope="class")
    def tests(self):
        return {"set.sql": tests__set_sql, "zip.sql": tests__zip_sql}

    def test_builtin_functions(self, project):
        assert len(run_dbt(["test"])) == 2


class TestContextBuiltinExceptions:
    # Assert compilation errors are raised with try_ equivalents
    def test_builtin_functions(self, project):
        write_file(tests__set_exception_sql, project.project_root, "models", "raise.sql")
        with pytest.raises(CompilationException):
            run_dbt(["compile"])

        write_file(tests__zip_exception_sql, project.project_root, "models", "raise.sql")
        with pytest.raises(CompilationException):
            run_dbt(["compile"])
