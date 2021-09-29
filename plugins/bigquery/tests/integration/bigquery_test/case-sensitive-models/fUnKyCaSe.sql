{{ config(materialized='incremental') }}
select 1 as id
{% if is_incremental() %}
this is a syntax error!
{% endif %}
