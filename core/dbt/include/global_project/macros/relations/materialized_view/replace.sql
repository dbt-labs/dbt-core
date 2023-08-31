{% macro get_replace_materialized_view_as_sql(relation, sql) %}
    {{- log('Applying REPLACE to: ' ~ relation) -}}
    {{- adapter.dispatch('get_replace_materialized_view_as_sql', 'dbt')(relation, sql) -}}
{% endmacro %}


{% macro default__get_replace_materialized_view_as_sql(relation, sql) %}
    {{ exceptions.raise_compiler_error("`get_replace_materialized_view_as_sql` has not been implemented for this adapter.") }}
{% endmacro %}
