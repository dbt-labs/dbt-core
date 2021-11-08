{% macro handle_existing_table(full_refresh, old_relation) %}
    {{ adapter.dispatch('handle_existing_table', 'dbt')(full_refresh, old_relation) }}
{% endmacro %}

{% macro default__handle_existing_table(full_refresh, old_relation) %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}
