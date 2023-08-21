{% macro postgres__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = postgres__describe_materialized_view(existing_relation) %}
    {% set _configuration_changes = existing_relation.get_materialized_view_config_change_collection(_existing_materialized_view, new_config) %}
    {% do return(_configuration_changes) %}
{% endmacro %}
