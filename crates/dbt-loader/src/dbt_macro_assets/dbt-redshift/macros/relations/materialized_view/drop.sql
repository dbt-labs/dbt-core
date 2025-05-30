{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }} cascade
{%- endmacro %}
