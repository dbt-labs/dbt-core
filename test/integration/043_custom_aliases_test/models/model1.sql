{{ config(materialized='table', alias='alias') }}

select {{ string_literal(this.name) }} as model_name
