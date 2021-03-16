
{{ config(materialized="incremental") }}

{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
    DECLARE int_var INT64 DEFAULT 42;

  	CREATE TEMPORARY FUNCTION a_to_b(str STRING)
	RETURNS STRING AS (
	  CASE
	  WHEN LOWER(str) = 'a' THEN 'b'
	  ELSE str
	  END
	);
{% endcall %}

select a_to_b(dupe) as dupe from {{ ref('view_model') }}
