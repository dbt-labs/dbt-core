models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    indexes=[
      {'columns': ['column_a'], 'type': 'hash'},
      {'columns': ['column_a', 'column_b'], 'unique': True},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__table_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_b']},
      {'columns': ['column_a', 'column_b']},
      {'columns': ['column_b', 'column_a'], 'type': 'btree', 'unique': True},
      {'columns': ['column_a'], 'type': 'hash'}
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_columns_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': 'column_a, column_b'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'type': 'non_existent_type'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_unique_config_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'unique': 'yes'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__missing_columns_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'unique': True},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

snapshots__colors_sql = """
{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id'], 'type': 'hash'},
              {'columns': ['id', 'color'], 'unique': True},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% else %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% endif %}

{% endsnapshot %}

"""

seeds__seed_csv = """country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""
