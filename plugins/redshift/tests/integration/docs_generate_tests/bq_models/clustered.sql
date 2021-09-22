{{
    config(
        materialized='table',
        partition_by={'field': 'updated_at', 'data_type': 'date'},
        cluster_by=['first_name']
    )
}}

select id,first_name,email,ip_address,DATE(updated_at) as updated_at from {{ ref('seed') }}
