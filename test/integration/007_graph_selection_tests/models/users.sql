
{{
    config(
        materialized = 'table',
        tags=['bi', 'users']
    )
}}

select * from {{ ref('base_users') }}
