{{ config(materialized='table') }}

select {{ string_literal(this.name) }} as model_name
