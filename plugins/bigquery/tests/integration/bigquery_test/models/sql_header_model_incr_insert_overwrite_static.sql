
{#
    Ensure that the insert overwrite incremental strategy
    works correctly when a UDF is used in a sql_header. The
    failure mode here is that dbt might inject the UDF header
    twice: once for the `create table` and then again for the
    merge statement.
#}

{{ config(
    materialized="incremental",
    incremental_strategy='insert_overwrite',
    partition_by={"field": "dt", "data_type": "date"},
    partitions=["'2020-01-1'"]
) }}

{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
  	CREATE TEMPORARY FUNCTION a_to_b(str STRING)
	RETURNS STRING AS (
	  CASE
	  WHEN LOWER(str) = 'a' THEN 'b'
	  ELSE str
	  END
	);
{% endcall %}

select
    cast('2020-01-01' as date) as dt,
    a_to_b(dupe) as dupe

from {{ ref('view_model') }}
