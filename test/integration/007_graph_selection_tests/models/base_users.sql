
{{
    config(
        materialized = 'ephemeral'
    )
}}

select * from "graph_selection_tests_007"."seed"
