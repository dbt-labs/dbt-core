import pytest

from dbt.tests.util import run_dbt, update_config_file
from dbt.exceptions import CompilationException

model_sql = """
select 1 as id
"""

bad_generate_macros__generate_names_sql = """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {% do var('somevar') %}
    {% do return(dbt.generate_schema_name(custom_schema_name, node)) %}
{%- endmacro %}

"""


class TestMissingVarGenerateNameMacro:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"generate_names.sql": bad_generate_macros__generate_names_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    def test_generate_schema_name_var(self, project):
        # var isn't set, so generate_name macro fails
        with pytest.raises(CompilationException) as excinfo:
            run_dbt(["compile"])

        assert "Required var 'somevar' not found in config" in str(excinfo.value)

        # globally scoped -- var is set at top-level
        update_config_file({"vars": {"somevar": 1}}, project.project_root, "dbt_project.yml")
        run_dbt(["compile"])

        # locally scoped -- var is set in 'test' scope
        update_config_file(
            {"vars": {"test": {"somevar": 1}}}, project.project_root, "dbt_project.yml"
        )
        run_dbt(["compile"])
