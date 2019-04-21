{% materialization table, adapter='snowflake' %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set backup_identifier = identifier + '__dbt_backup' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database, type='table') -%}

  /*
      See ../view/view.sql for more information about this relation.
  */

  -- drop the backup relation if it exists, then make a new one that uses the old relation's type
  {%- set backup_relation = adapter.get_relation(database=database, schema=schema, identifier=backup_identifier) -%}
  {% if backup_relation is not none -%}
    {{ adapter.drop_relation(backup_relation) }}
  {%- endif %}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema,
                                                database=database,
                                                type=(old_relation.type or 'table')) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set create_as_temporary = (exists_as_table and non_destructive_mode) -%}


  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}

  -- setup: if the target relation already exists, truncate or drop it (if it's a view)
  {% if non_destructive_mode -%}
    {% if exists_as_table -%}
      --noop we can do away with this step all together since the table can be replaced in Snowflake.
      {# {{ adapter.truncate_relation(old_relation) }} #}
    {% elif exists_as_view -%}
      --noop. I think we should also be able to do away with this and call a replace.
      {{ adapter.drop_relation(old_relation) }}
      {%- set old_relation = none -%}
    {%- endif %}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  --build model
  {% call statement('main') -%}
  -- we can leverage Snowflake create or replace table here to achieve an atomic replace.
    {% if old_relation is not none %}
      {# -- I'm preserving one of the old checks here for a view, and to make sure Snowflake doesn't
      -- complain that we're running a replace table on a view. #}
      {% if old_relation.type == 'view' %}
        {{ log("Dropping relation " ~ old_relation ~ " because it is a view and this model is a table.") }}
        {{ drop_relation_if_exists(old_relation) }}
      {% endif %}
    {% endif %}
    -- 
    {{ log("Using snowflake create or replace method.") }}
    {{snowflake__create_or_replace_table_as(target_relation, sql)}}
  {%- endcall %}

  -- skiping all previous renames here since they are not needed in Snowflake

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {# -- TODO: Check with Drew wether this backup_relation gets used at all should this materialisation
  -- fail #}
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
