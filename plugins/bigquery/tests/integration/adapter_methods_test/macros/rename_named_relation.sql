-- Macro to rename a relation
{% macro rename_named_relation(from_name, to_name) %}
{%- set from_relation = api.Relation.create(database=target.database, schema=target.schema, identifier=from_name, type='table') -%}
{%- set to_relation = api.Relation.create(database=target.database, schema=target.schema, identifier=to_name, type='table') -%}
{% do adapter.rename_relation(from_relation, to_relation) %}
{% endmacro %}