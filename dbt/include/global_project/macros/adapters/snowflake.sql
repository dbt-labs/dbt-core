{% macro snowflake__create_table_as(temporary, identifier, sql) -%}
  {% if temporary %}
    use schema "{{ schema }}";
  {% endif %}

  create {% if temporary: -%}temporary{%- endif %} table
    {% if not temporary: -%}"{{ schema }}".{%- endif %}"{{ identifier }}" as (
    {{ sql }}
  );
{% endmacro %}
