{% macro get_create_materialized_view_as_sql(relation, sql) %}
    {{ adapter.dispatch('get_create_materialized_view_as_sql', 'dbt')(relation, sql) }}
{% endmacro %}


{% macro default__get_create_materialized_view_as_sql(relation, sql) %}

    create materialized view {{ relation }}
    as (
        {{ sql }}
    );

    {{ create_indexes(relation) }}

{% endmacro %}
