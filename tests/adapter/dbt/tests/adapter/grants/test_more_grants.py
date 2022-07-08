import pytest
from dbt.tests.util import run_dbt

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()


seeds_added_csv = (
    seeds_base_csv
    + """
11,Mateo,2014-09-07T17:04:27
12,Julian,2000-02-04T11:48:30
13,Gabriel,2001-07-10T07:32:52
14,Isaac,2002-11-24T03:22:28
15,Levi,2009-11-15T11:57:15
16,Elizabeth,2005-04-09T03:50:11
17,Grayson,2019-08-06T19:28:17
18,Dylan,2014-03-01T11:50:41
19,Jayden,2009-06-06T07:12:49
20,Luke,2003-12-05T21:42:18
""".lstrip()
)

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
seeds:
  - name: base
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
  - name: added
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
models:
  - name: incremental
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

incremental_sql = """
{{ config(materialized="incremental") }}
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
  where id > (select max(id) from {{ this }})
{% endif %}
"""


class BaseSeedsIncrementalGrants:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incr_grants"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "added.csv": seeds_added_csv}

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1


class TestSeedsIncrementalGrants(BaseSeedsIncrementalGrants):
    pass


cc_all_snapshot_sql = """
{% snapshot cc_all_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

snapshot_schema_yml = """
version: 2
seeds:
  - name: cc_all_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""


class BaseSnapshotGrants:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_grants"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cc_all_snapshot.sql": cc_all_snapshot_sql,
        }

    def test_snapshot_grants(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # snapshot command
        results = run_dbt(["snapshot"])
        for result in results:
            assert result.status == "success"

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"


class TestSnapshotGrants(BaseSnapshotGrants):
    pass
