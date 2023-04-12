{% macro get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {{- adapter.dispatch('get_materialized_view_configuration_changes', 'dbt')(existing_relation, new_config) -}}
{% endmacro %}


{% macro default__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set updates = [] %}
    {{ return(updates) }}
{% endmacro %}
