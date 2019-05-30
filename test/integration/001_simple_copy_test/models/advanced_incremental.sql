{{
  config(
    materialized = "incremental",
    unique_key = "id",
    persist_docs = {"relation": true}
  )
}}


select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
