{{
  config(
    materialized='table',
    persist_docs={ 'columns': true }
  )
}}

select
  1 field
