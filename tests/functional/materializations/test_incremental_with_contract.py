import pytest

from dbt.tests.util import (
    check_relations_equal,
    get_relation_columns,
    relation_from_name,
    run_dbt,
)

seeds_base_csv = """
id,name_xxx,some_date
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

incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
    1 || '-' || current_timestamp as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""

incremental_sql = """
  {{ config(materialized="incremental") }}
  select * from {{ source('raw', 'seed') }}
  {% if is_incremental() %}
  where id > (select max(id) from {{ this }})
  {% endif %}
"""

schema_base_yml = """
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"

models:
  - name: incremental
    config:
      contract:
        enforced: true
      on_schema_change: append_new_columns
    columns:
      - name: id
        data_type: int
      - name: name_xxx
        data_type: character varying(10)
      - name: some_date
        data_type: timestamp
"""


class TestIncremental:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

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

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--debug", "--vars", "seed_name: added"])
        assert len(results) == 1

        # Error before fix:  Changing col type from character varying(10) to character varying(256) in table:
        #  "dbt"."test<...>_test_incremental_with_contract"."incremental"
        columns = get_relation_columns(project.adapter, "incremental")
        # [('id', 'integer', None), ('name_xxx', 'character varying', 10), ('some_date', 'timestamp without time zone', None)]
        for column in columns:
            if column[0] == "name_xxx":
                assert column[2] == 10
