{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['alias'] -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set old_relation = adapter.get_relation(
      schema=schema, identifier=identifier) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema,
      type='view') -%}

  {{ run_hooks(pre_hooks) }}

  -- If there's a table with the same name and we weren't told to full refresh,
  -- that's an error. If we were told to full refresh, drop it.
  {%- if old_relation is not none and old_relation.is_table -%}
    {%- if flags.FULL_REFRESH and not non_destructive_mode -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- else -%}
      {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {%- endif -%}
  {%- endif -%}

  -- build model
  {% if exists_as_view and non_destructive_mode -%}
    {% call noop_statement('main', status="PASS", res=None) -%}
      -- Not running : non-destructive mode
      {{ sql }}
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_view_as(target_relation, sql) }}
    {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks) }}

{%- endmaterialization %}
