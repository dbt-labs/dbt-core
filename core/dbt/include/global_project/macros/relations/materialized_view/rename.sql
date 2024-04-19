{% macro get_rename_materialized_view_sql(relation, new_name) %}
    {% if relation is not none %}
        {% set database = relation.database %}
        {% set schema = relation.schema %}
        {% set to_relation = adapter.get_relation(database=database, schema=schema, identifier=new_name) %}}
        {% if to_relation is not none %}
            {{ adapter.rename_relation(from_relation=relation, to_relation=to_relation) }}
        {% endif %}
    {% endif %}
    {{- adapter.dispatch('get_rename_materialized_view_sql', 'dbt')(relation, new_name) -}}
{% endmacro %}


{% macro default__get_rename_materialized_view_sql(relation, new_name) %}
    {{ exceptions.raise_compiler_error(
        "`get_rename_materialized_view_sql` has not been implemented for this adapter."
    ) }}
{% endmacro %}
