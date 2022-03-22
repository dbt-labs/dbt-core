-- a unique key list with any element not in the model itself should error out

{{
    config(
        materialized='incremental',
        unique_key=['state', 'thisisnotacolumn']
    )
}}

select * from {{ ref('seed') }}
