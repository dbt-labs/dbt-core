{{
    config(
        materialized='view', bind=False
    )
}}

select * from {{ ref('seed') }}
