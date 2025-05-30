{% macro can_clone_table() %}
    {{ return(adapter.dispatch('can_clone_table', 'dbt')()) }}
{% endmacro %}

{% macro default__can_clone_table() %}
    {{ return(False) }}
{% endmacro %}
