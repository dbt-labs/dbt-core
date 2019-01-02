{{
  config(
    materialized = "incremental",
    sql_where = "id>(select max(id) from {{this}})"
  )
}}

select * from {{ this.schema }}.seed

{% if adapter.already_exists(this) %}

    where id > (select max(id) from {{this}})

{% endif %}
