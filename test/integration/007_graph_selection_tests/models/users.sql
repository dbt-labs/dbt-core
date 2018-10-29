
{{
    config(
        materialized = 'table',
        tags='bi'
    )
}}

select * from {{ ref('base_users') }}
