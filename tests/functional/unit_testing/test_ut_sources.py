import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.contracts.results import RunStatus, TestStatus

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
