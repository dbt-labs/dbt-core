{%- materialization view, default -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}
  {%- set should_ignore = non_destructive_mode and existing_type == 'view' %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ drop_if_exists(existing, schema, tmp_identifier) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if should_ignore -%}
    {#
      -- Materializations need to a statement with name='main'.
      -- We could issue a no-op query here (like `select 1`), but that's wasteful. Instead:
      --   1) write the sql contents out to the compiled dirs
      --   2) return a status and result to the caller
    #}
    {% call noop_statement('main', status="PASS", res=None) -%}
      -- Not running : non-destructive mode
      {{ sql }}
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_view_as(tmp_identifier, sql) }}
    {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- cleanup
  {% if not should_ignore -%}
    {{ drop_if_exists(existing, schema, identifier) }}
    {{ adapter.rename(schema, tmp_identifier, identifier) }}
  {%- endif %}

  {#
      -- Don't commit in non-destructive mode _unless_ there are in-transaction hooks
      -- TODO : Figure out some other way of doing this that isn't as fragile
  #}
  {% if has_transactional_hooks or not should_ignore %}
      {{ adapter.commit() }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization -%}
