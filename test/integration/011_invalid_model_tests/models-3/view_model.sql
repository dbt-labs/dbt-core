{{
  config(
    enabled = False
  )
}}

select * from {{ this.schema }}.seed
