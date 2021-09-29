{{
  config(
    materialized = "view",
    persist_docs={ "relation": true, "columns": true, "schema": true }
  )
}}


select
    id,
    current_date as updated_at,
    dupe

from {{ source('raw', 'seed') }}
