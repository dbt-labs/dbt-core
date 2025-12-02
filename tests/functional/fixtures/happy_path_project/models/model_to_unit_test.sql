{{ config(materialized='table') }}

SELECT * FROM {{ ref('seed')}}
