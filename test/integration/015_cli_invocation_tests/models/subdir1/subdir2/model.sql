{{
    config(
        materialized='table'
    )
}}

{# we don't care what, just do anything that will fail without "dbt deps" #}
{% do dependency.some_macro() %}

select 1 as id
