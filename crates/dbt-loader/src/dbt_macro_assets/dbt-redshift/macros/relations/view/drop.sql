{%- macro redshift__drop_view(relation) -%}
    drop view if exists {{ relation }}{% if not adapter.has_feature('drop_without_cascade') %} cascade{% endif %}
{%- endmacro -%}
