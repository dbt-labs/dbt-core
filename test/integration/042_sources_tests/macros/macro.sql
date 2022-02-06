{% macro override_me() -%}
    {{ exceptions.raise_compiler_error('this is a bad macro') }}
{%- endmacro %}

{% macro happy_little_macro() -%}
    {{ override_me() }}
{%- endmacro %}


{% macro vacuum_source(source_name, table_name) -%}
    {% call statement('stmt', auto_begin=false, fetch_result=false) %}
        vacuum {{ source(source_name, table_name) }}
    {% endcall %}
{%- endmacro %}
