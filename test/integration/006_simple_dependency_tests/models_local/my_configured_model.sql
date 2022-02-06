{{
    config(schema='configured')
}}
select * from {{ ref('model_to_import') }}
