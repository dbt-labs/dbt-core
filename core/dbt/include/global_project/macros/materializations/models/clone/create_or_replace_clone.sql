{% macro create_or_replace_clone(this_relation, state_relation) %}
    {{ return(adapter.dispatch('create_or_replace_clone', 'dbt')(this_relation, state_relation)) }}
{% endmacro %}

{% macro default__create_or_replace_clone(this_relation, state_relation) %}
    create or replace table {{ this_relation }} clone {{ state_relation }}
{% endmacro %}
