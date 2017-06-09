{% macro dbt__create_view(schema, model, sql, flags, adapter) -%}

  {%- set identifier = model['name'] -%}

  create view "{{ schema }}"."{{ identifier }}__dbt_tmp" as (
      {{ sql }}
  );
  
{%- endmacro %}
