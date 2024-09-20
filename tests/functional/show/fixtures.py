models__sample_model = """
select * from {{ ref('sample_seed') }}
"""

models__sample_number_model = """
select
  cast(1.0 as int) as float_to_int_field,
  3.0 as float_field,
  4.3 as float_with_dec_field,
  5 as int_field
"""

models__sample_number_model_with_nulls = """
select
  cast(1.0 as int) as float_to_int_field,
  3.0 as float_field,
  4.3 as float_with_dec_field,
  5 as int_field

union all

select
  cast(null as int) as float_to_int_field,
  cast(null as float) as float_field,
  cast(null as float) as float_with_dec_field,
  cast(null as int) as int_field

"""

models__second_model = """
select
    sample_num as col_one,
    sample_bool as col_two,
    42 as answer
from {{ ref('sample_model') }}
"""

models__sql_header = """
{% call set_sql_header(config) %}
set session time zone '{{ var("timezone", "Europe/Paris") }}';
{%- endcall %}
select current_setting('timezone') as timezone
"""

private_model_yml = """
groups:
  - name: my_cool_group
    owner: {name: me}

models:
  - name: private_model
    access: private
    config:
      group: my_cool_group
"""


schema_yml = """
models:
  - name: sample_model
    latest_version: 1

    # declare the versions, and fully specify them
    versions:
      - v: 2
        config:
          materialized: table
        columns:
          - name: sample_num
            data_type: int
          - name: sample_bool
            data_type: bool
          - name: answer
            data_type: int

      - v: 1
        config:
          materialized: table
          contract: {enforced: true}
        columns:
          - name: sample_num
            data_type: int
          - name: sample_bool
            data_type: bool
"""

models__ephemeral_model = """
{{ config(materialized = 'ephemeral') }}
select
    coalesce(sample_num, 0) + 10 as col_deci
from {{ ref('sample_model') }}
"""

models__second_ephemeral_model = """
{{ config(materialized = 'ephemeral') }}
select
    col_deci + 100 as col_hundo
from {{ ref('ephemeral_model') }}
"""

seeds__sample_seed = """sample_num,sample_bool
1,true
2,false
3,true
4,false
5,true
6,false
7,true
"""
