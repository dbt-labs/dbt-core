-- Macro to rename a relation
{% macro rename_named_relation(from_name, to_name) %}
{%- set from_relation = adapter.get_relation(database=target.database, schema=target.schema, identifier=from_name) -%}
{%- set to_relation = adapter.get_relation(database=target.database, schema=target.schema, identifier=to_name) -%}
{% do adapter.rename_relation(from_relation, to_relation) %}
{% endmacro %}