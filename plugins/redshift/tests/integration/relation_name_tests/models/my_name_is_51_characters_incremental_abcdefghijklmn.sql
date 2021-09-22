
select * from {{ this.schema }}.seed

{{
  config({
    "unique_key": "col_A",
    "materialized": "incremental"
    })
}}
