{%- macro current_timestamp() -%}
    {{ adapter.dispatch('current_timestamp', 'dbt')() }}
{%- endmacro -%}

{% macro default__current_timestamp() -%}
  {{ exceptions.raise_not_implemented(
    'current_timestamp macro not implemented for adapter ' + adapter.type()) }}
{%- endmacro %}

{%- macro snapshot_get_time() -%}
    {{ adapter.dispatch('snapshot_get_time', 'dbt')() }}
{%- endmacro -%}

{% macro default__snapshot_get_time() %}
    {{ current_timestamp() }}
{% endmacro %}

---------------------------------------------

/* {#
    DEPRECATED: DO NOT USE IN NEW PROJECTS

    This is ONLY to handle the fact that Snowflake + Postgres had functionally
    different implementations of {{ dbt.current_timestamp }} + {{ dbt_utils.current_timestamp }}

    If you had a project or package that called {{ dbt_utils.current_timestamp() }}, you should
    continue to use this macro to guarantee identical behavior on those two databases.
#} */

{% macro current_timestamp_backcompat() %}
    {{ return(adapter.dispatch('current_timestamp_backcompat', 'dbt')()) }}
{% endmacro %}

{% macro default__current_timestamp_backcompat() %}
    current_timestamp::timestamp
{% endmacro %}

{% macro current_timestamp_in_utc_backcompat() %}
    {{ return(adapter.dispatch('current_timestamp_in_utc_backcompat', 'dbt')()) }}
{% endmacro %}

{% macro default__current_timestamp_in_utc_backcompat() %}
    {{ return(adapter.dispatch('current_timestamp_backcompat', 'dbt')()) }}
{% endmacro %}
