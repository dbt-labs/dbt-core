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
