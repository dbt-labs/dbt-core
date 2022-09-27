{%- macro current_timestamp() -%}
    {{ adapter.dispatch('current_timestamp', 'dbt')() }}
{%- endmacro -%}

{% macro default__current_timestamp() %}
    current_timestamp
{% endmacro %}

{%- macro current_timestamp_in_utc() -%}
    {{ adapter.dispatch('current_timestamp_in_utc', 'dbt')() }}
{%- endmacro -%}

{% macro default__current_timestamp_in_utc() %}
    {{ convert_timezone(target_tz="UTC",
        source_timestamp=current_timestamp())}}
{% endmacro %}

{%- macro snapshot_get_time() -%}
    {{ adapter.dispatch('snapshot_get_time', 'dbt')() }}
{%- endmacro -%}

{% macro default__snapshot_get_time() %}
    {{ current_timestamp() }}
{% endmacro %}

{%- macro convert_timezone(source_tz, target_tz, source_timestamp) -%}
    {%- if not target_tz is string -%}
        {{ exceptions.raise_compiler_error("'target_tz' must be a string") }}
    {%- else -%}
        {{ adapter.dispatch('convert_timezone', 'dbt') (source_tz, target_tz, source_timestamp) }}
    {%- endif -%}

{%- endmacro -%}

{%- macro default__convert_timezone(source_tz, target_tz, source_timestamp) -%}
    {%- if not source_tz -%}
        {{ source_timestamp }} at time zone '{{ target_tz }}'
    {%- else -%}
        {{ source_timestamp }} at time zone '{{ source_tz }}' at time zone '{{ target_tz }}'
    {%- endif -%}
{%- endmacro -%}


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
    {{ return(adapter.dispatch('current_timestamp', 'dbt')()) }}
{% endmacro %}

{% macro current_timestamp_in_utc_backcompat() %}
    {{ return(adapter.dispatch('default__current_timestamp_in_utc_backcompat', 'dbt')()) }}
{% endmacro %}

{% macro default__current_timestamp_in_utc_backcompat() %}
    {{ return(adapter.dispatch('current_timestamp_in_utc', 'dbt')()) }}
{% endmacro %}
