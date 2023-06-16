{% macro get_clone_target(to_relation) %}
    {{ return(adapter.dispatch('get_clone_target', 'dbt')(to_relation)) }}
{% endmacro %}

{% macro default__get_clone_target(to_relation) %}
    {% set target_sql %}
        select * from {{ to_relation }}
    {% endset %}
    {{ return(target_sql) }}
{% endmacro %}
