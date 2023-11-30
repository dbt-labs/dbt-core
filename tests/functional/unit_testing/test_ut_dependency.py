import pytest
from dbt.tests.util import run_dbt, get_unique_ids_in_results
from dbt.tests.fixtures.project import write_project_files

local_dependency__dbt_project_yml = """

name: 'local_dep'
version: '1.0'

seeds:
  quote_columns: False

"""

local_dependency__schema_yml = """
sources:
  - name: seed_source
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: "seed"
        columns:
          - name: id
            tests:
              - unique

unit_tests:
  - name: test_dep_model_id
    model: dep_model
    given:
      - input: ref('seed')
        rows:
          - {id: 1, name: Joe}
    expect:
      rows:
        - {name_id: Joe_1}


"""

local_dependency__dep_model_sql = """
select name || '_' || id as name_id  from {{ ref('seed') }}

"""

local_dependency__seed_csv = """id,name
1,Mary
2,Sam
3,John
"""


class TestUnitTestingInDependency:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        local_dependency_files = {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "models": {
                "schema.yml": local_dependency__schema_yml,
                "dep_model.sql": local_dependency__dep_model_sql,
            },
            "seeds": {"seed.csv": local_dependency__seed_csv},
        }
        write_project_files(project_root, "local_dependency", local_dependency_files)

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_dependency"}]}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as fun",
        }

    def test_unit_test_in_dependency(self, project):
        run_dbt(["deps"])
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])
        assert len(results) == 2
        unique_ids = get_unique_ids_in_results(results)
        assert "unit_test.local_dep.dep_model.test_dep_model_id" in unique_ids
