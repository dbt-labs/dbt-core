
{{
    config(materialized='ephemeral', tags=['base'])
}}

select distinct email from {{ ref('base_users') }}
