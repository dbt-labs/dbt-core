from copy import deepcopy

import pytest

from dbt.contracts.results import RunStatus, TestStatus
from dbt.tests.util import run_dbt, write_file

raw_customers_csv = """id,first_name,last_name,email
1,Michael,Perez,mperez0@chronoengine.com
2,Shawn,Mccoy,smccoy1@reddit.com
3,Kathleen,Payne,kpayne2@cargocollective.com
4,Jimmy,Cooper,jcooper3@cargocollective.com
5,Katherine,Rice,krice4@typepad.com
6,Sarah,Ryan,sryan5@gnu.org
7,Martin,Mcdonald,mmcdonald6@opera.com
8,Frank,Robinson,frobinson7@wunderground.com
9,Jennifer,Franklin,jfranklin8@mail.ru
10,Henry,Welch,hwelch9@list-manage.com
"""

schema_sources_yml = """
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            data_tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email
unit_tests:
  - name: test_customers
    model: customers
    given:
      - input: source('seed_sources', 'raw_customers')
        rows:
          - {id: 1, first_name: Emily}
    expect:
      rows:
        - {id: 1, first_name: Emily}
"""

customers_sql = """
select * from {{ source('seed_sources', 'raw_customers') }}
"""

failing_test_schema_yml = """
  - name: fail_test_customers
    model: customers
    given:
      - input: source('seed_sources', 'raw_customers')
        rows:
          - {id: 1, first_name: Emily}
    expect:
      rows:
        - {id: 1, first_name: Joan}
"""


schema_duplicate_source_names_yml = """
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
  - name: seed_sources_2
    schema: "{{ target.schema }}_other"
    tables:
      - name: raw_customers

unit_tests:
  - name: test_customers
    model: customers_duplicate_source_names
    given:
      - input: source('seed_sources', 'raw_customers')
        rows:
          - {id: 1, first_name: Emily}
      - input: source('seed_sources_2', 'raw_customers')
        rows:
          - {id: 2, first_name: Michelle}
    expect:
      rows:
        - {id: 1, first_name: Emily}
        - {id: 2, first_name: Michelle}
"""

customers_duplicate_source_names_sql = """
select * from {{ source('seed_sources', 'raw_customers') }}
union all
select * from {{ source('seed_sources_2', 'raw_customers') }}
"""


class TestUnitTestSourceInput:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_customers.csv": raw_customers_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customers.sql": customers_sql,
            "sources.yml": schema_sources_yml,
        }

    def test_source_input(self, project):
        results = run_dbt(["seed"])
        results = run_dbt(["run"])
        len(results) == 1

        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1

        results = run_dbt(["build"])
        assert len(results) == 5
        result_unique_ids = [result.node.unique_id for result in results]
        assert len(result_unique_ids) == 5
        assert "unit_test.test.customers.test_customers" in result_unique_ids

        # write failing unit test
        write_file(
            schema_sources_yml + failing_test_schema_yml,
            project.project_root,
            "models",
            "sources.yml",
        )
        results = run_dbt(["build"], expect_pass=False)
        for result in results:
            if result.node.unique_id == "model.test.customers":
                assert result.status == RunStatus.Skipped
            elif result.node.unique_id == "unit_test.test.customers.fail_test_customers":
                assert result.status == TestStatus.Fail
        assert len(results) == 6


class TestUnitTestSourceInputSameNames:
    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_customers.csv": raw_customers_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customers_duplicate_source_names.sql": customers_duplicate_source_names_sql,
            "sources.yml": schema_duplicate_source_names_yml,
        }

    def test_source_input_same_names(self, project, other_schema):
        results = run_dbt(["seed"])

        project.create_test_schema(schema_name=other_schema)
        results = run_dbt(["seed", "--target", "otherschema"])

        # results = run_dbt(["run"])
        # len(results) == 1

        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
