

-- hack b/c bq model names are fully qualified, which doesn't work
-- with query_for_existing
{% macro test_was_materialized(model, name, type) %}

    {#-- don't run this query in the parsing step #}
    {%- if model -%}
        {%- set existing_tables = adapter.query_for_existing(schema) -%}
    {%- else -%}
        {%- set existing_tables = {} -%}
    {%- endif -%}

    {% if name in existing_tables and existing_tables[name] == type %}
        select 0 as success
    {% else %}
        select 1 as error
    {% endif %}

{% endmacro %}
