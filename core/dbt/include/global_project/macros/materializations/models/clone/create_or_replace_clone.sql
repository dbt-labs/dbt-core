{% macro create_or_replace_clone(this_relation, defer_relation) %}
    {{ return(adapter.dispatch('create_or_replace_clone', 'dbt')(this_relation, defer_relation)) }}
{% endmacro %}

{% macro default__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace table {{ this_relation }} clone {{ defer_relation }}
{% endmacro %}
