{{ config(materialized='ephemeral') }}

select * from "ephemeral_019"."seed"
