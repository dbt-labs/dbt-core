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

seeds_newcolumns_csv = """
id,name,some_date,last_initial
1,Easton,1981-05-20T06:46:51,A
2,Lillian,1978-09-03T18:10:33,B
3,Jeremiah,1982-03-11T03:59:51,C
4,Nolan,1976-05-06T20:21:35,D
5,Hannah,1982-06-23T05:41:26,E
6,Eleanor,1991-08-10T23:12:21,F
7,Lily,1971-03-29T14:58:02,G
8,Jonathan,1988-02-26T02:55:24,H
9,Adrian,1994-02-09T13:14:23,I
10,Nora,1976-03-01T16:51:39,J
""".lstrip()

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

generic_test_seed_yml = """
version: 2
models:
  - name: base
    columns:
     - name: id
       tests:
         - not_null
"""

generic_test_view_yml = """
version: 2
models:
  - name: view_model
    columns:
     - name: id
       tests:
         - not_null
"""

generic_test_table_yml = """
version: 2
models:
  - name: table_model
    columns:
     - name: id
       tests:
         - not_null
"""

test_passing_sql = """
select * from (
    select 1 as id
) as my_subquery
where id = 2
"""

test_failing_sql = """
select * from (
    select 1 as id
) as my_subquery
where id = 1
"""

test_ephemeral_passing_sql = """
with my_other_cool_cte as (
    select id, name from {{ ref('ephemeral') }}
    where id > 1000
)
select name, id from my_other_cool_cte
"""

test_ephemeral_failing_sql = """
with my_other_cool_cte as (
    select id, name from {{ ref('ephemeral') }}
    where id < 1000
)
select name, id from my_other_cool_cte
"""

model_incremental = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

cc_all_snapshot_sql = """
{% snapshot cc_all_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

cc_name_snapshot_sql = """
{% snapshot cc_name_snapshot %}
    {{ config(
        check_cols=['name'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

cc_date_snapshot_sql = """
{% snapshot cc_date_snapshot %}
    {{ config(
        check_cols=['some_date'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

ts_snapshot_sql = """
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
    )}}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

model_ephemeral_with_cte = """
with my_cool_cte as (
  select name, id from {{ ref('base') }}
)
select id, name from my_cool_cte where id is not null
"""

config_materialized_table = """
  {{ config(materialized="table") }}
"""

config_materialized_view = """
  {{ config(materialized="view") }}
"""

config_materialized_ephemeral = """
  {{ config(materialized="ephemeral") }}
"""

config_materialized_incremental = """
  {{ config(materialized="incremental") }}
"""

config_materialized_var = """
  {{ config(materialized=var("materialized_var", "table"))}}
"""

model_base = """
  select * from {{ source('raw', 'seed') }}
"""

model_ephemeral = """
  select * from {{ ref('ephemeral') }}
"""

base_materialized_var_sql = config_materialized_var + model_base
base_table_sql = config_materialized_table + model_base
base_view_sql = config_materialized_view + model_base
base_ephemeral_sql = config_materialized_ephemeral + model_base
ephemeral_with_cte_sql = config_materialized_ephemeral + model_ephemeral_with_cte
ephemeral_view_sql = config_materialized_view + model_ephemeral
ephemeral_table_sql = config_materialized_table + model_ephemeral
incremental_sql = config_materialized_incremental + model_incremental
