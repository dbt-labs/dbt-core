{% set col_a = '"col_a"' %}
{% set col_b = '"col_b"' %}
{% if adapter.type() == 'bigquery' %}
    {% set col_a = '`col_a`' %}
    {% set col_b = '`col_b`' %}
{% elif adapter.type() == 'snowflake' %}
	{% set col_a = '"COL_A"' %}
	{% set col_b = '"COL_B"' %}
{% endif %}

{{config(
    materialized = 'incremental',
    unique_key = col_a,
    incremental_strategy = var('strategy')
    )}}

select
{{ col_a }}, {{ col_b }}
from {{ref('seed')}}
