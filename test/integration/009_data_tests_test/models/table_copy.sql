
{{
    config(
        materialized='table'
    )
}}

select * from "data_tests_009"."seed"
