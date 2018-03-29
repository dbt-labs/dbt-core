{% macro snowflake__create_table_as(temporary, identifier, sql) -%}
  {% if temporary %}
    use schema {{ schema }};
  {% endif %}

  create {% if temporary: -%}temporary{%- endif %} table
    {% if not temporary: -%}{{ schema }}.{%- endif %}{{ identifier }} as (
    {{ sql }}
  );
{% endmacro %}


{% macro snowflake__get_existing_relation_type(existing, identifier) -%}
  {%- set upcased_existing = {} -%}
  {%- for k,v in existing.items() -%}
    {%- set _ = upcased_existing.update({k.upper(): v}) -%}
  {%- endfor -%}

  {%- set existing_type = upcased_existing.get(identifier.upper()) -%}
  {{ return(existing_type) }}
{%- endmacro %}
