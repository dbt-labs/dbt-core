{{ config(materialized='table') }}

select bad_column from {{ ref('snap_0') }}