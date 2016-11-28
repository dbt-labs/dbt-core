
{{
    config(
        materialized='table'
    )
}}

select * from schema_tests_008.seed_failure
