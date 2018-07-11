{{
  config(
    materialized = "table",
    sort = 'first_name',
    dist = 'first_name',
    clusterby = 'first_name',
  )
}}

select * from {{ this.schema }}.seed
