{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
  	create or replace table {{ ref('table_model') }} as (
		select * from {{ ref('table_model') }}
		)
{% endcall %}

select * from {{ ref('view_model') }}