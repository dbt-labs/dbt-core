{% materialization table, adapter='snowflake' %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- Drop the relation if it was a view to "convert" it in a table. This may lead to
    -- downtime, but it should be a relatively infrequent occurrence  #}
  {% if old_relation is not none and not old_relation.is_table %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ drop_relation_if_exists(old_relation) }}
  {% endif %}

  --build model
  {% call statement('main') -%}
    {{ create_table_as(false, target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- add in table comments
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}

  {%- set table_comment = get_relation_comment(raw_persist_docs, model) -%}
  {%- set column_comment = get_relation_column_comments(raw_persist_docs, model) -%}

  {{ alter_relation_comment(target_relation, table_comment) }}
  {{ alter_column_comment(target_relation, column_comment) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
