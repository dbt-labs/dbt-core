{{ config(materialized='ephemeral') }}
select * from no.such.table
