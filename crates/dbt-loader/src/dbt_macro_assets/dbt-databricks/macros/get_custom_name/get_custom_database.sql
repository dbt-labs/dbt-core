{#
    This is identical to the implementation in dbt-core.
    We need to override because dbt-spark overrides to something we don't like.
#}

{% macro databricks__generate_database_name(custom_database_name=none, node=none) -%}
    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}
        {{ return(default_database) }}
    {%- else -%}
        {{ return(custom_database_name) }}
    {%- endif -%}
{%- endmacro %}
