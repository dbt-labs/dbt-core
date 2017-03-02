{{ config(materialized='ephemeral') }}

select * from "ephemeral_020"."seed"
