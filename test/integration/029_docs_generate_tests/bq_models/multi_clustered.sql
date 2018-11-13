{{
    config(
        materialized='table',
        partition_by='updated_at',
        cluster_by=['first_name','email']
    )
}}

select id,first_name,email,ip_address,DATE(updated_at) as updated_at from {{ ref('seed') }}
