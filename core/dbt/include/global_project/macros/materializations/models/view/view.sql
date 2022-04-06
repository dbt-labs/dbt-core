{%- materialization view, default -%}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,
                                                type='view') -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}
  {%- set temp_relation = temp_relation.incorporate(path={"schema": schema,
                                                         "database": database}) -%}

  -- the temp_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_temp_relation = adapter.get_relation(identifier=temp_relation['identifier'],
                                                           schema=schema,
                                                           database=database) -%}
  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "old_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the old_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the old_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation) -%}

  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = adapter.get_relation(identifier=backup_relation['identifier'],
                                                             schema=schema,
                                                             database=database) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_temp_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(temp_relation, sql) }}
  {%- endcall %}

  -- cleanup
  -- move the existing view out of the way
  {% if old_relation is not none %}
    {{ adapter.rename_relation(old_relation, backup_relation) }}
  {% endif %}
  {{ adapter.rename_relation(temp_relation, target_relation) }}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
