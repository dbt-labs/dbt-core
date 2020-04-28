{{
  config ({
  	"materialized" : 'incremental',
    "persist_docs" : { "relation": true, "columns": true, "schema": true }
  })
}}

select 1 as column1
