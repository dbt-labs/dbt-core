{% macro sql_server__create_view_as(identifier, sql) -%}
  {% set to_hoist %}
    create view {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }} as
    {{ sql }}
  {% endset %}
  {{ hoist_ctes(to_hoist) }}
{% endmacro %}

{% macro sql_server__create_table_as(temporary, identifier, sql) -%}
  {% set to_hoist %}
    with intermediate as ( {{ sql }} )
    select *
    into {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }}
    from intermediate
  {% endset %}
  {{ hoist_ctes(to_hoist) }}
{% endmacro %}
