import pytest
import yaml

from dbt.artifacts.schemas.results import RunStatus
from dbt.exceptions import DbtProjectError
from dbt.tests.util import relation_from_name, run_dbt
from dbt_common.exceptions import CompilationError

# =============================================================================
# Fixtures - Models, Macros, etc.
# =============================================================================

# SQL model that uses a var
model_with_var_sql = """
{{ config(materialized='table') }}
select '{{ var("my_var") }}' as my_var_value
"""

# SQL model that uses multiple vars
model_with_multiple_vars_sql = """
{{ config(materialized='table') }}
select
    '{{ var("var_one") }}' as var_one_value,
    '{{ var("var_two") }}' as var_two_value
"""

# SQL model that uses a package-scoped var
model_with_package_var_sql = """
{{ config(materialized='table') }}
select '{{ var("package_var") }}' as package_var_value
"""

# SQL model that calls a macro which uses a var
model_calling_macro_sql = """
{{ config(materialized='table') }}
select '{{ get_var_value() }}' as macro_var_value
"""

# Macro that uses a var
macro_with_var_sql = """
{% macro get_var_value() -%}
{{ var("macro_var") }}
{%- endmacro %}
"""


class TestDbtProjectVarFromVarsFile:
    """dbt_project.yml with var should be rendered properly when var is set through vars.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+meta": {
                    "project_var": "{{ var('project_var') }}",
                }
            }
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"my_var": "from_file", "project_var": "project_var_from_file"}}

    def test_dbt_project_var_from_vars_file(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == RunStatus.Success


class TestDbtProjectVarCliOverridesFile:
    """dbt_project.yml with var should use CLI value when set in both vars.yml and cli"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+meta": {
                    "project_var": "{{ var('project_var') }}",
                }
            }
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"my_var": "from_file", "project_var": "from_file"}}

    def test_cli_overrides_vars_file(self, project):
        cli_vars = {"my_var": "from_cli", "project_var": "from_cli"}
        results = run_dbt(["run", "--vars", yaml.safe_dump(cli_vars)])
        assert len(results) == 1
        # Verify CLI value was used
        relation = relation_from_name(project.adapter, "model_with_var")
        result = project.run_sql(f"select my_var_value from {relation}", fetch="one")
        assert result[0] == "from_cli"


class TestDbtProjectVarMissingFromVarsFile:
    """dbt should throw error if dbt_project.yml expects a var not present in vars.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+meta": {
                    "missing_var": "{{ var('missing_var') }}",
                }
            }
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"my_var": "from_file"}}

    def test_error_when_var_missing(
        self, project_root, profiles_root, profiles_yml, dbt_project_yml
    ):
        # This test expects an error during project setup, so we can't use the project fixture.
        # We must pass --project-dir and --profiles-dir explicitly since the adapter fixture
        # (which normally sets these flags) is not used.
        with pytest.raises(
            CompilationError, match="Required var 'missing_var' not found in config"
        ):
            run_dbt(
                [
                    "run",
                    "--project-dir",
                    str(project_root),
                    "--profiles-dir",
                    str(profiles_root),
                ],
                expect_pass=False,
            )


class TestSqlModelVarFromVarsFile:
    """SQL model with variable should be rendered properly when var is set through vars.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {"vars": {"my_var": "sql_var_from_file"}}

    def test_sql_model_var_from_file(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_with_var")
        result = project.run_sql(f"select my_var_value from {relation}", fetch="one")
        assert result[0] == "sql_var_from_file"


class TestSqlModelVarCliOverridesFile:
    """SQL model with variable should use CLI value when set in both vars.yml and cli"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def vars_yml_udpate(self, project_root):
        return {"vars": {"my_var": "from_file"}}

    def test_sql_model_cli_overrides_file(self, project):
        cli_vars = {"my_var": "from_cli"}
        results = run_dbt(["run", "--vars", yaml.safe_dump(cli_vars)])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_with_var")
        result = project.run_sql(f"select my_var_value from {relation}", fetch="one")
        assert result[0] == "from_cli"


class TestSqlModelVarMissingFromVarsFile:
    """dbt should throw an error if sql model expects a var not set in vars.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        # Model expects 'my_var' but it won't be provided
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def vars_yml(self, project_root):
        return {"vars": {"other_var": "value"}}

    def test_error_when_model_var_missing(self, project):
        # run_dbt with expect_pass=False doesn't raise, it returns results with errors
        results = run_dbt(["run"], expect_pass=False)
        # The run should have an error for the missing var
        assert len(results) == 1
        assert results[0].status == "error"


class TestMacroVarFromVarsFile:
    """Macro using var should be rendered properly when var is set through vars.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_calling_macro.sql": model_calling_macro_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macro.sql": macro_with_var_sql}

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {"vars": {"macro_var": "macro_var_from_file"}}

    def test_macro_var_from_file(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_calling_macro")
        result = project.run_sql(f"select macro_var_value from {relation}", fetch="one")
        assert result[0] == "macro_var_from_file"


class TestMacroVarCliOverridesFile:
    """Macro using var should use CLI value when set in both vars.yml and cli"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_calling_macro.sql": model_calling_macro_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macro.sql": macro_with_var_sql}

    @pytest.fixture(scope="class")
    def vars_yml(self, project_root):
        return {"vars": {"macro_var": "from_file"}}

    def test_macro_var_cli_overrides_file(self, project):
        cli_vars = {"macro_var": "from_cli"}
        results = run_dbt(["run", "--vars", yaml.safe_dump(cli_vars)])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_calling_macro")
        result = project.run_sql(f"select macro_var_value from {relation}", fetch="one")
        assert result[0] == "from_cli"


class TestMutualExclusivityError:
    """Project should throw error when variables are set in both vars.yml and dbt_project.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            "vars": {
                "my_var": "from_project",
            },
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {"vars": {"my_var": "from_file"}}

    def test_error_when_both_have_vars(
        self, project_root, profiles_root, profiles_yml, dbt_project_yml
    ):
        # run_dbt catches exceptions, so we use run_dbt_and_capture to check output
        with pytest.raises(
            DbtProjectError,
            match="Variables cannot be defined in both vars.yml and dbt_project.yml.",
        ):
            run_dbt(
                [
                    "run",
                    "--project-dir",
                    str(project_root),
                    "--profiles-dir",
                    str(profiles_root),
                ],
                expect_pass=False,
            )


class TestEmptyVarsFileAllowsProjectVars:
    """If vars.yml file is empty, vars from dbt_project.yml should be used"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "my_var": "from_project",
            }
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {}

    def test_empty_vars_file_uses_project_vars(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_with_var")
        result = project.run_sql(f"select my_var_value from {relation}", fetch="one")
        assert result[0] == "from_project"


class TestVarsFileWithoutVarsKeyAllowsProjectVars:
    """Vars declared in vars.yml without a top level 'vars' key should use vars from dbt_project.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_var.sql": model_with_var_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "my_var": "from_project",
            }
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {"other_key": "some_value"}

    def test_vars_file_without_vars_key_uses_project_vars(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_with_var")
        result = project.run_sql(f"select my_var_value from {relation}", fetch="one")
        assert result[0] == "from_project"


class TestPartialCliOverride:
    """Variables from vars.yml and CLI should be merged"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_with_multiple_vars.sql": model_with_multiple_vars_sql}

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {
            "vars": {
                "var_two": "var_two_from_file",
            }
        }

    def test_partial_cli_override(self, project):
        # Only override var_one, var_two should come from file
        cli_vars = {"var_one": "var_one_from_cli"}
        results = run_dbt(["run", "--vars", yaml.safe_dump(cli_vars)])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "model_with_multiple_vars")
        result = project.run_sql(
            f"select var_one_value, var_two_value from {relation}", fetch="one"
        )
        assert result[0] == "var_one_from_cli"  # From CLI
        assert result[1] == "var_two_from_file"  # From file


class TestComplexVarValues:
    """Complex var values like lists and dicts should work from vars.yml"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_list_var.sql": """
{{ config(materialized='table') }}
select '{{ var("list_var") | join(",") }}' as list_value
""",
            "model_with_dict_var.sql": """
{{ config(materialized='table') }}
select '{{ var("dict_var").key1 }}' as dict_value
""",
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self, project_root):
        return {
            "vars": {
                "list_var": ["a", "b", "c"],
                "dict_var": {"key1": "value1", "key2": "value2"},
            }
        }

    def test_complex_var_values(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        list_relation = relation_from_name(project.adapter, "model_with_list_var")
        list_result = project.run_sql(f"select list_value from {list_relation}", fetch="one")
        assert list_result[0] == "a,b,c"

        dict_relation = relation_from_name(project.adapter, "model_with_dict_var")
        dict_result = project.run_sql(f"select dict_value from {dict_relation}", fetch="one")
        assert dict_result[0] == "value1"
