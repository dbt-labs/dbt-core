{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }}{% if not adapter.has_feature('drop_without_cascade') %} cascade{% endif %}
{%- endmacro %}
