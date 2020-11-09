
select * from {{ this.schema }}.seed

{{
  config({
    "materialized": "table"
    })
}}
