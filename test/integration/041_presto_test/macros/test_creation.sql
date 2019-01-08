
{% macro test_was_materialized(model, name, type) %}

    {#-- don't run this query in the parsing step #}
    {%- if model -%}
        {%- set table = adapter.get_relation(database=model.database, schema=model.schema,
                                             identifier=model.name) -%}
    {%- else -%}
        {%- set table = {} -%}
    {%- endif -%}

    {% if table and table.type == type %}
        select 0 as success
    {% else %}
        select 1 as error
    {% endif %}

{% endmacro %}
