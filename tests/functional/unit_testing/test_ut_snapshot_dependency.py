import pytest
from dbt.tests.util import run_dbt
from dbt.contracts.results import RunStatus, TestStatus

raw_customers_csv = """id,first_name,last_name,email,gender,ip_address,updated_at
1,'Judith','Kennedy','(not provided)','Female','54.60.24.128','2015-12-24 12:19:28'
2,'Arthur','Kelly','(not provided)','Male','62.56.24.215','2015-10-28 16:22:15'
3,'Rachel','Moreno','rmoreno2@msu.edu','Female','31.222.249.23','2016-04-05 02:05:30'
4,'Ralph','Turner','rturner3@hp.com','Male','157.83.76.114','2016-08-08 00:06:51'
5,'Laura','Gonzales','lgonzales4@howstuffworks.com','Female','30.54.105.168','2016-09-01 08:25:38'
6,'Katherine','Lopez','klopez5@yahoo.co.jp','Female','169.138.46.89','2016-08-30 18:52:11'
7,'Jeremy','Hamilton','jhamilton6@mozilla.org','Male','231.189.13.133','2016-07-17 02:09:46'
"""

top_level_domains_csv = """id,domain
3,'msu.edu'
4,'hp.com'
5,'howstuffworks.com'
6,'yahoo.co.jp'
7,'mozilla.org'
"""

snapshots_users__snapshot_sql = """
{% snapshot snapshot_users %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='check',
            check_cols=['email'],
        )
    }}
    select *, split_part(email, '@', 2) as domain from {{target.database}}.{{schema}}.raw_customers

{% endsnapshot %}
"""

unit_test_yml = """
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: top_level_domains
        columns:
          - name: id
          - name: domain

unit_tests:
  - name: test_is_valid_email_address
    model: customers
    given:
      - input: ref('snapshot_users')
        rows:
         - {id: 1, email: cool@example.com,     domain: example.com}
         - {id: 2, email: cool@unknown.com,     domain: unknown.com}
         - {id: 3, email: badgmail.com,         domain: gmailcom}
         - {id: 4, email: missingdot@gmailcom,  domain: gmailcom}
      - input: source('seed_sources', 'top_level_domains')
        rows:
         - {domain: example.com}
         - {domain: gmail.com}
    expect:
      rows:
        - {id: 1, is_valid_email_address: true}
        - {id: 2, is_valid_email_address: false}
        - {id: 3, is_valid_email_address: false}
        - {id: 4, is_valid_email_address: false}

  - name: fail_is_valid_email_address
    model: customers
    given:
      - input: ref('snapshot_users')
        rows:
        - {id: 1, email: cool@example.com,     domain: example.com}
      - input: source('seed_sources', 'top_level_domains')
        rows:
        - {domain: example.com}
        - {domain: gmail.com}
    expect:
      rows:
        - {id: 1, is_valid_email_address: false}
"""

customers_sql = """
with snapshot_users as (
select * from {{ ref('snapshot_users') }}
),

top_level_domains as (
select * from {{ source('seed_sources', 'top_level_domains') }}
),
matched_values as (
    select
        snapshot_users.*,
        case when exists (
            select 1 from top_level_domains
            where top_level_domains.domain = snapshot_users.domain
        ) then true else false end as is_valid_email_address
    from
        snapshot_users
)

select * from matched_values
"""


class TestUnitTestSnapshotDependency:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_customers.csv": raw_customers_csv,
            "top_level_domains.csv": top_level_domains_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customers.sql": customers_sql,
            "unit_tests.yml": unit_test_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_users.sql": snapshots_users__snapshot_sql,
        }

    def test_snapshot_dependency(self, project):
        seed_results = run_dbt(["seed"])
        len(seed_results) == 2
        snapshot_results = run_dbt(["snapshot"])
        len(snapshot_results) == 1
        model_results = run_dbt(["run"])
        len(model_results) == 1

        # test passing unit test
        results = run_dbt(["test", "--select", "test_name:test_is_valid_email_address"])
        assert len(results) == 1

        # test failing unit test
        results = run_dbt(
            ["test", "--select", "test_name:fail_is_valid_email_address"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].status == TestStatus.Fail

        # test all with build
        results = run_dbt(["build"], expect_pass=False)

        for result in results:
            if result.node.unique_id == "unit_test.test.customers.fail_is_valid_email_address":
                # This will always fail, regarless of order executed
                assert result.status == TestStatus.Fail
            elif result.node.unique_id == "unit_test.test.customers.test_is_valid_email_address":
                # there's no guarantee that the order of the results will be the same.  If the
                # failed test runs first this one gets skipped.  If this runs first it passes.
                assert result.status in [TestStatus.Pass, TestStatus.Skipped]
            elif result.node.unique_id == "model.test.customers":
                # This is always skipped because one test always fails
                assert result.status == RunStatus.Skipped
        assert len(results) == 6
