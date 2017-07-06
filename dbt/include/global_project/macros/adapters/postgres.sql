{% macro postgres__create_table_as(temporary, identifier, sql) -%}
  create {% if temporary: -%}temporary{%- endif %} table
    {% if not temporary: -%}"{{ schema }}".{%- endif %}"{{ identifier }}" as (
    {{ sql }}
  );
{% endmacro %}
