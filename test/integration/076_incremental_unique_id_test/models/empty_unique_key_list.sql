-- model with empty list unique key should build normally

{{
    config(
        materialized='incremental',
        unique_key=[]
    )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}
