{{ config(materialized='ephemeral') }}

{{ adapter.invalid_method() }}

select * from {{ ref('base') }}
