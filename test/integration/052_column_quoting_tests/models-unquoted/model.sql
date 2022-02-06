{% set col_a = '"col_a"' %}
{% set col_b = '"col_b"' %}

{{config(
    materialized = 'incremental',
    unique_key = col_a,
    incremental_strategy = var('strategy')
    )}}

select
{{ col_a }}, {{ col_b }}
from {{ref('seed')}}
