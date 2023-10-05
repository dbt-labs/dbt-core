import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationError

model_sql = """
select 1 as id
"""

fail_macros__failure_sql = """
{% macro get_catalog_relations(information_schema, relations) %}
    {% do exceptions.raise_compiler_error('rejected: no catalogs for you') %}
{% endmacro %}

"""


class TestDocsGenerateOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"failure.sql": fail_macros__failure_sql}

    def test_override_used(
        self,
        project,
    ):
        results = run_dbt(["run"])
        assert len(results) == 1
        # this should pick up our failure macro and raise a compilation exception
        with pytest.raises(CompilationError) as excinfo:
            run_dbt(["--warn-error", "docs", "generate"])
        assert "rejected: no catalogs for you" in str(excinfo.value)
