{{ config(materialized='ephemeral') }}

select
  1 as id,
  {{ dbt.date_trunc('day', dbt.current_timestamp()) }} as created_at
