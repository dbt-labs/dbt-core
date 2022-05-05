import pytest
import yaml

from dbt.tests.util import run_dbt, get_artifact, write_config_file
from dbt.exceptions import RuntimeException, CompilationException


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

really_simple_model_sql = """
select 'abc' as simple
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


class TestCLIVarsProfile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": really_simple_model_sql,
        }

    def test_cli_vars_in_profile(self, project, dbt_profile_data):
        profile = dbt_profile_data
        profile["test"]["outputs"]["default"]["host"] = "{{ var('db_host') }}"
        write_config_file(profile, project.profiles_dir, "profiles.yml")
        with pytest.raises(RuntimeException):
            results = run_dbt(["run"])
        results = run_dbt(["run", "--vars", "db_host: localhost"])
        assert len(results) == 1


class TestCLIVarsPackages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": really_simple_model_sql,
        }

    @pytest.fixture(scope="class")
    def packages_config(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "1.1",
                }
            ]
        }

    def test_cli_vars_in_packages(self, project, packages_config):
        # Run working deps and run commands
        run_dbt(["deps"])
        results = run_dbt(["run"])
        assert len(results) == 1

        # Change packages.yml to contain a var
        packages = packages_config
        packages["packages"][0]["revision"] = "{{ var('dip_version') }}"
        write_config_file(packages, project.project_root, "packages.yml")

        # Without vars args deps fails
        with pytest.raises(RuntimeException):
            run_dbt(["deps"])

        # With vars arg deps succeeds
        results = run_dbt(["deps", "--vars", "dip_version: 1.1"])
        assert results is None


initial_selectors_yml = """
selectors:
  - name: dev_defer_snapshots
    default: "{{ target.name == 'dev' | as_bool }}"
    definition:
      method: fqn
      value: '*'
      exclude:
        - method: config.materialized
          value: snapshot
"""

var_selectors_yml = """
selectors:
  - name: dev_defer_snapshots
    default: "{{ var('snapshot_target') == 'dev' | as_bool }}"
    definition:
      method: fqn
      value: '*'
      exclude:
        - method: config.materialized
          value: snapshot
"""


class TestCLIVarsSelectors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": really_simple_model_sql,
        }

    @pytest.fixture(scope="class")
    def selectors(self):
        return initial_selectors_yml

    def test_vars_in_selectors(self, project):
        # initially runs ok
        results = run_dbt(["run"])
        assert len(results) == 1

        # Update the selectors.yml file to have a var
        write_config_file(var_selectors_yml, project.project_root, "selectors.yml")
        with pytest.raises(CompilationException):
            run_dbt(["run"])

        # Var in cli_vars works
        results = run_dbt(["run", "--vars", "snapshot_target: dev"])
        assert len(results) == 1
