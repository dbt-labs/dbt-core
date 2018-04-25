{{
  config(
    materialized = "incremental",
    sql_where = "TRUE"
  )
}}


select *
from {{ ref('seed') }}

{% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}

    where id > (select max(id) from {{this}})

{% endif %}
