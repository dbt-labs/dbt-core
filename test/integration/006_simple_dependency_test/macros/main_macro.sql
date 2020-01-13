{# This macro also exists in the dependency -dbt should be fine with that #}
{% macro some_overridden_macro() -%}
999
{%- endmacro %}
