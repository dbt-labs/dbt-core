import pytest
import yaml

from dbt.tests.util import run_dbt, get_artifact


models_complex__schema_yml = """
version: 2
models:
- name: complex_model
  columns:
  - name: var_1
    tests:
    - accepted_values:
        values:
        - abc
  - name: var_2
    tests:
    - accepted_values:
        values:
        - def
  - name: var_3
    tests:
    - accepted_values:
        values:
        - jkl
"""

models_complex__complex_model_sql = """
select
    '{{ var("variable_1") }}'::varchar as var_1,
    '{{ var("variable_2")[0] }}'::varchar as var_2,
    '{{ var("variable_3")["value"] }}'::varchar as var_3
"""

models_simple__schema_yml = """
version: 2
models:
- name: simple_model
  columns:
  - name: simple
    tests:
    - accepted_values:
        values:
        - abc
"""

models_simple__simple_model_sql = """
select
    '{{ var("simple") }}'::varchar as simple
"""


class TestCLIVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_complex__schema_yml,
            "complex_model.sql": models_complex__complex_model_sql,
        }

    def test__cli_vars_longform(self, project):
        cli_vars = {
            "variable_1": "abc",
            "variable_2": ["def", "ghi"],
            "variable_3": {"value": "jkl"},
        }
        results = run_dbt(["run", "--vars", yaml.dump(cli_vars)])
        assert len(results) == 1
        results = run_dbt(["test", "--vars", yaml.dump(cli_vars)])
        assert len(results) == 3


class TestCLIVarsSimple:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": models_simple__simple_model_sql,
        }

    def test__cli_vars_shorthand(self, project):
        results = run_dbt(["run", "--vars", "simple: abc"])
        assert len(results) == 1
        results = run_dbt(["test", "--vars", "simple: abc"])
        assert len(results) == 1

    def test__cli_vars_longer(self, project):
        results = run_dbt(["run", "--vars", "{simple: abc, unused: def}"])
        assert len(results) == 1
        results = run_dbt(["test", "--vars", "{simple: abc, unused: def}"])
        assert len(results) == 1
        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert run_results["args"]["vars"] == "{simple: abc, unused: def}"
