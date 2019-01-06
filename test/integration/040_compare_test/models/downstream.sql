{{ config(materialized='view') }}

SELECT * FROM {{ ref('test_view') }}
