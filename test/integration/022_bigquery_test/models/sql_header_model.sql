{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
  	create or replace table {{ ref('table_model') }}
  	OPTIONS(
		persist_docs={ "relation": true, "columns": true, "schema": true })
  	as (
		select * from {{ ref('view_model') }}
  	  )
{% endcall %}

select * from {{ ref('view_model') }}