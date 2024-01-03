{% macro get_rename_table_sql(relation, new_name) %}
    {{- adapter.dispatch('get_rename_table_sql', 'dbt')(relation, new_name) -}}
{% endmacro %}


{% macro default__get_rename_table_sql(relation, new_name) %}
    {{ exceptions.raise_compiler_error(
        "`get_rename_table_sql` has not been implemented for this adapter."
    ) }}
{% endmacro %}
