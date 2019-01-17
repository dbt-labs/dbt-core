{{
  config(
    materialized = "incremental",
    sql_where = "TRUE",
    unique_key = "id"
  )
}}


select *
from {{ ref('seed') }}

{% if adapter.already_exists(this) and not flags.FULL_REFRESH %}

    where id > (select max(id) from {{this}})

{% endif %}
