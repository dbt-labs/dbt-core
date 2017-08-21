{%- materialization view, default -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ drop_if_exists(existing, tmp_identifier) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if non_destructive_mode and existing_type == 'view' -%}
    -- noop
  {%- else -%}
    {% call statement('main') -%}
      {{ create_view_as(tmp_identifier, sql) }}
    {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- cleanup
  {% if non_destructive_mode and existing_type == 'view' -%}
    -- noop
  {%- else -%}
    {{ drop_if_exists(existing, identifier) }}
    {{ adapter.rename(tmp_identifier, identifier) }}
  {%- endif %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization -%}
