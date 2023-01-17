import pytest

bad_sql = """
select bad sql here
"""

dupe_sql = """
select 1 as id, current_date as updated_at
union all
select 2 as id, current_date as updated_at
union all
select 3 as id, current_date as updated_at
union all
select 4 as id, current_date as updated_at
"""

good_sql = """
select 1 as id, current_date as updated_at
union all
select 2 as id, current_date as updated_at
union all
select 3 as id, current_date as updated_at
union all
select 4 as id, current_date as updated_at
"""

snapshots_good_sql = """
{% snapshot good_snapshot %}
    {{ config(target_schema=schema, target_database=database, strategy='timestamp', unique_key='id', updated_at='updated_at')}}
    select * from {{ schema }}.good
{% endsnapshot %}
"""

snapshots_bad_sql = """
{% snapshot good_snapshot %}
    {{ config(target_schema=schema, target_database=database, strategy='timestamp', unique_key='id', updated_at='updated_at_not_real')}}
    select * from {{ schema }}.good
{% endsnapshot %}
"""

schema_yml = """
version: 2
models:
- name: good
  columns:
  - name: updated_at
    tests:
    - not_null
- name: bad
  columns:
  - name: updated_at
    tests:
    - not_null
- name: dupe
  columns:
  - name: updated_at
    tests:
    - unique
"""

data_seed_good_csv = """a,b,c
1,2,3
"""

data_seed_bad_csv = """a,b,c
1,\2,3,a,a,a
"""


class BaseConfigProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad.sql": bad_sql,
            "dupe.sql": dupe_sql,
            "good.sql": good_sql,
            "schema.yml": schema_yml,
        }
