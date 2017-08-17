
{{
    config(materialized='ephemeral')
}}

select distinct email from {{ ref('base_users') }}
