{{
  config(
    materialized = "table",
    persist_docs={ "relation": true, "columns": true, "schema": true }
  )
}}

select * from {{ ref('view_model') }}
