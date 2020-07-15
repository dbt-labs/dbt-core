{{ config(materialized='view') }}
SELECT
    STRUCT(
        STRUCT(
            1 AS level_3_a,
            2 AS level_3_b
        ) AS level_2
    ) AS level_1