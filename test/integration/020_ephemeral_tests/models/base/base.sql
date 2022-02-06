{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed
