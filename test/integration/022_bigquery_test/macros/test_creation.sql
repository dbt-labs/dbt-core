
{% test was_materialized(model, name, type) %}

    {#-- don't run this query in the parsing step #}
    {%- if model -%}
        {%- set table = adapter.get_relation(database=model.database, schema=model.schema,
                                             identifier=model.name) -%}
    {%- else -%}
        {%- set table = {} -%}
    {%- endif -%}

    {% if table %}
      select '{{ table.type }} does not match expected value {{ type }}'
      from (select true)
      where '{{ table.type }}' != '{{ type }}'
    {% endif %}

{% endtest %}
