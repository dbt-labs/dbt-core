{% materialization table, default %}
  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {{ drop_if_exists(existing, tmp_identifier) }}

  -- setup
  {% if non_destructive_mode -%}
    {% if existing_type == 'table' -%}
      {{ adapter.truncate(identifier) }}
    {% elif existing_type == 'view' -%}
      {{ adapter.drop(identifier, existing_type) }}
    {%- endif %}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {%- if non_destructive_mode -%}
      {%- if adapter.already_exists(schema, identifier) -%}
        {{ create_table_as(True, tmp_identifier, sql) }}

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ schema }}.{{ identifier }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from "{{ tmp_identifier }}"
        );
      {%- else -%}
        {{ create_table_as(False, identifier, sql) }}
      {%- endif -%}
    {%- else -%}
      {{ create_table_as(False, tmp_identifier, sql) }}
    {%- endif -%}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- cleanup
  {% if non_destructive_mode -%}
    -- noop
  {%- else -%}
    {{ drop_if_exists(existing, identifier) }}
    {{ adapter.rename(tmp_identifier, identifier) }}
  {%- endif %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
