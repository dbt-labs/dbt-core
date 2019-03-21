{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
