-- a unique key with a string should trigger to overwrite behavior when
--   the source has entries in conflict (i.e. more than one row per unique key
--   combination)

{{
    config(
        materialized='incremental',
        unique_key='state'
    )
}}

select
    state::varchar(2) as state,
    county::varchar(12) as county,
    city::varchar(12) as city,
    last_visit_date::date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}
