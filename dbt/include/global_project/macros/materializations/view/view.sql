{%- materialization view, default -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set existing_relations = adapter.list_relations(schema=schema) -%}
  {%- set old_relation = adapter.get_relation(relations_list=existing_relations,
                                              schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema,
                                                type='view') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, type='view') -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}
  {%- set should_ignore = non_destructive_mode and exists_as_view %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ adapter.drop_relation(intermediate_relation) }}

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
      {{ create_view_as(intermediate_relation, sql) }}
    {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- cleanup
  {% if not should_ignore -%}
    {{ drop_relation_if_exists(old_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
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
