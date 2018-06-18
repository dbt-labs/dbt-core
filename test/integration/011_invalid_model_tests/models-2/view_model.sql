{{
  config(
    enabled = 'false'
  )
}}

select * from {{ this.schema }}.seed
