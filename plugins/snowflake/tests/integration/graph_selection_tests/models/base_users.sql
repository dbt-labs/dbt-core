
{{
    config(
        materialized = 'ephemeral',
        tags = ['base']
    )
}}

select * from {{ source('raw', 'seed') }}
