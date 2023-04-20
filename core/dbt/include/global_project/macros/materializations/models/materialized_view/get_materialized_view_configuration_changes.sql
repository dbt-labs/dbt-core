{% macro get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {{- log('Determining configuration changes on: ' ~ existing_relation) -}}
    {%- do return(adapter.dispatch('get_materialized_view_configuration_changes', 'dbt')(existing_relation, new_config)) -%}
{% endmacro %}


{% macro default__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {%- do return([]) -%}
{% endmacro %}
