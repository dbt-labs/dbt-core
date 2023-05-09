{% macro get_materialized_view_configuration_changes(existing_relation, new_config) %}
    -- TODO: move this from jinja into python, it is not a template and does not need to be overwritten
    {{- log('Determining configuration changes on: ' ~ existing_relation) -}}
    {%- do return(adapter.dispatch('get_materialized_view_configuration_changes', 'dbt')(existing_relation, new_config)) -%}
{% endmacro %}


{% macro default__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
