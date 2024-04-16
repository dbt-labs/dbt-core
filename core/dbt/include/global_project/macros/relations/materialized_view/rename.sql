{% macro get_rename_materialized_view_sql(relation, new_name) %}
    {%- set to_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=new_name) %}
    {{- adapter.dispatch('get_rename_materialized_view_sql', 'dbt')(relation, new_name) -}}
     {%- if relation is not none and  to_relation is not none -%}
      {{ adapter.cache_renamed(from_relation=relation, to_relation=to_relation) }}
    {%- endif -%}
{% endmacro %}


{% macro default__get_rename_materialized_view_sql(relation, new_name) %}
    {{ exceptions.raise_compiler_error(
        "`get_rename_materialized_view_sql` has not been implemented for this adapter."
    ) }}
{% endmacro %}
