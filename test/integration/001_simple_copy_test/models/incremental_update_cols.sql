{{
    config(
        materialized = "incremental",
        unique_key = "id",
        incremental_update_columns = ["email", "ip_address"]
    )
}}


select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
