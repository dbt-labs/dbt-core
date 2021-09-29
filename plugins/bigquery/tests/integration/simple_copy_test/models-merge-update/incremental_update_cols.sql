{{
    config(
        materialized = "incremental",
        unique_key = "id",
        merge_update_columns = ["email", "ip_address"]
    )
}}


select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where load_date > (select max(load_date) from {{this}})

{% endif %}
