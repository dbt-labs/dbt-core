import pytest

from dbt.tests.fixtures.project import write_project_files

first_dependency__dbt_project_yml = """
name: 'first_dep'
version: '1.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

require-dbt-version: '>=0.1.0'

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
    - "target"
    - "dbt_packages"

vars:
  first_dep:
    first_dep_global: 'first_dep_global_value_overridden'
    test_config_root_override: 'configured_from_dependency'
    test_config_package: 'configured_from_dependency'

seeds:
  quote_columns: True

"""

first_dependency__models__nested__first_dep_model_sql = """
select
    '{{ var("first_dep_global") }}' as first_dep_global,
    '{{ var("from_root_to_first") }}' as from_root
"""

first_dependency__seeds__first_dep_expected_csv = """first_dep_global,from_root
first_dep_global_value_overridden,root_first_value
"""

first_dependency__models__nested__first_dep_model_var_expected_csv = """test_config_root_override,test_config_package
configured_from_root,configured_from_dependency
"""

first_dependency__models__nested__first_dep_model_var_sql = """
select
    '{{ config.get("test_config_root_override") }}' as test_config_root_override,
    '{{ config.get("test_config_package") }}' as test_config_package
"""

first_dependency__model_var_in_config_schema = """
models:
- name: first_dep_model
  config:
    test_config_root_override: "{{ var('test_config_root_override') }}"
    test_config_package: "{{ var('test_config_package') }}"
"""


class FirstDependencyProject:
    @pytest.fixture(scope="class")
    def first_dependency(self, project):
        first_dependency_files = {
            "dbt_project.yml": first_dependency__dbt_project_yml,
            "models": {
                "nested": {
                    "first_dep_model.sql": first_dependency__models__nested__first_dep_model_sql
                }
            },
            "seeds": {"first_dep_expected.csv": first_dependency__seeds__first_dep_expected_csv},
        }
        write_project_files(project.project_root, "first_dependency", first_dependency_files)


class FirstDependencyConfigProject:
    @pytest.fixture(scope="class")
    def first_dependency(self, project):
        first_dependency_files = {
            "dbt_project.yml": first_dependency__dbt_project_yml,
            "models": {
                "nested": {
                    "first_dep_model.sql": first_dependency__models__nested__first_dep_model_var_sql,
                    "schema.yml": first_dependency__model_var_in_config_schema,
                }
            },
            "seeds": {
                "first_dep_expected.csv": first_dependency__models__nested__first_dep_model_var_expected_csv
            },
        }
        write_project_files(project.project_root, "first_dependency", first_dependency_files)
