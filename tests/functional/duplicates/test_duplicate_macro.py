import pytest

from dbt.exceptions import CompilationError
from dbt.tests.util import run_dbt


bad_same_macros_sql = """
{% macro some_macro() %}
{% endmacro %}

{% macro some_macro() %}
{% endmacro %}

"""

bad_separate_one_sql = """
{% macro some_macro() %}
{% endmacro %}

"""

bad_separate_two_sql = """
{% macro some_macro() %}
{% endmacro %}

"""

model_sql = """
select 1 as value
"""


class TestDuplicateMacroEnabledSameFile:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macro.sql": bad_same_macros_sql,
        }

    def test_duplicate_macros(self, project):
        message = 'dbt found two macros named "some_macro" in the project'
        with pytest.raises(CompilationError) as exc:
            run_dbt(["parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        assert message in exc_str
        assert "macro.sql" in exc_str


class TestDuplicateMacroEnabledDifferentFiles:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "one.sql": bad_separate_one_sql,
            "two.sql": bad_separate_two_sql,
        }

    def test_duplicate_macros(self, project):
        message = 'dbt found two macros named "some_macro" in the project'
        with pytest.raises(CompilationError) as exc:
            run_dbt(["compile"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        assert message in exc_str
        assert "one.sql" in exc_str
        assert "two.sql" in exc_str
