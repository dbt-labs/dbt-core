{{
  config(
    materialized = "view"
  )
}}


select
    id,
    current_date as updated_at,
    dupe

from {{ ref('data_seed') }}
