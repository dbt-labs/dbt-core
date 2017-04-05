{% macro dbt__create_view(schema, model, sql, flags, adapter) -%}

  {%- set identifier = model['name'] -%}
  {%- set already_exists = adapter.already_exists(schema, identifier) -%}
  {%- set non_destructive_mode = flags.NON_DESTRUCTIVE == True -%}

  {%- if non_destructive_mode -%}
    create view "{{ schema }}"."{{ identifier }}" as (
        {{ sql }}
    );
  {%- else -%}
    create view "{{ schema }}"."{{ identifier }}__dbt_tmp" as (
        {{ sql }}
    );
  {%- endif %}

{%- endmacro %}
