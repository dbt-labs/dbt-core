
{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- if existing_type is not none -%}
    {%- if existing_type == 'table' and not flags.FULL_REFRESH -%}
      {# this is only intended for date partitioned tables, but we cant see that field in the context #}
      {% set error_message -%}
        Trying to create model '{{ identifier }}' as a view, but it already exists as a table.
        Either drop the '{{ schema }}.{{ identifier }}' table manually, or use --full-refresh
      {%- endset %}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% if existing_type == 'view' and non_destructive_mode -%}
    {% call noop_statement('main', status="PASS", res=None) -%}
      -- Not running : non-destructive mode
      {{ sql }}
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_view_as(identifier, sql) }}
    {%- endcall %}
  {%- endif %}

{%- endmaterialization %}
