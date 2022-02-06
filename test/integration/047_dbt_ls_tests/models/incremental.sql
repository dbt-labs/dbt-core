{{
  config(
    materialized = "incremental",
    incremental_strategy = "delete+insert",
  )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where a > (select max(a) from {{this}})
{% endif %}
