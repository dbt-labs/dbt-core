{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}
    {% do exceptions.raise_compiler_error("is_incremental() evaluated to True in a table") %}
{% endif %}
