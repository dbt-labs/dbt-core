import pytest

from dbt.tests.util import run_dbt


models_override__schema_yml = """
version: 2
models:
- name: test_vars
  columns:
  - name: field
    tests:
    - accepted_values:
        values:
        - override
"""

models_override__test_vars_sql = """
select '{{ var("required") }}'::varchar as field
"""


# Tests that cli vars override vars set in the project config
class TestCLIVarOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_override__schema_yml,
            "test_vars.sql": models_override__test_vars_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "required": "present",
            },
        }

    def test__override_vars_global(self, project):
        run_dbt(["run", "--vars", "{required: override}"])
        run_dbt(["test"])


# This one switches to setting a var in 'test'
class TestCLIVarOverridePorject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_override__schema_yml,
            "test_vars.sql": models_override__test_vars_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "test": {
                    "required": "present",
                },
            },
        }

    def test__override_vars_project_level(self, project):

        # This should be "override"
        run_dbt(["run", "--vars", "{required: override}"])
        run_dbt(["test"])
