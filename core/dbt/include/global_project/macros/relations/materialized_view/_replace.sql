{# /*
This only exists for backwards compatibility for 1.6.0. In later versions, the general `get_replace_sql`
macro is called as replace is inherently not limited to a single relation (it takes in two relations).
*/ #}

{% macro get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{- log('Applying REPLACE to: ' ~ relation) -}}
    {{- adapter.dispatch('get_replace_materialized_view_as_sql', 'dbt')(relation, sql, existing_relation, backup_relation, intermediate_relation) -}}
{% endmacro %}


{% macro default__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
