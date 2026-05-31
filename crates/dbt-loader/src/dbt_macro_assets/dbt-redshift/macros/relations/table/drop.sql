{%- macro redshift__drop_table(relation) -%}
    drop table if exists {{ relation }}{% if not adapter.has_feature('drop_without_cascade') %} cascade{% endif %}
{%- endmacro -%}
