{% macro databricks__drop_view(relation) -%}
    drop view if exists {{ relation.render() }}
{%- endmacro %}
