{% materialization table, default %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set backup_identifier = identifier + '__dbt_backup' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database,
                                                      type='table') -%}

  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema,
                                                database=database,
                                                type=(old_relation.type or 'table')) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set create_as_temporary = (exists_as_table and non_destructive_mode) -%}


  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}
  {{ adapter.drop_relation(backup_relation) }}

  -- setup: if the target relation already exists, truncate or drop it (if it's a view)
  {% if non_destructive_mode -%}
    {% if exists_as_table -%}
      {{ adapter.truncate_relation(old_relation) }}
    {% elif exists_as_view -%}
      {{ adapter.drop_relation(old_relation) }}
      {%- set old_relation = none -%}
    {%- endif %}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {%- if non_destructive_mode -%}
      {%- if old_relation is not none -%}
        {{ create_table_as(create_as_temporary, intermediate_relation, sql) }}

        {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ target_relation }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from {{ intermediate_relation.include(database=(not create_as_temporary),
                                                schema=(not create_as_temporary)) }}
        );
      {%- else -%}
        {{ create_table_as(create_as_temporary, target_relation, sql) }}
      {%- endif -%}
    {%- else -%}
      {{ create_table_as(create_as_temporary, intermediate_relation, sql) }}
    {%- endif -%}
  {%- endcall %}

  -- cleanup
  {% if non_destructive_mode -%}
    -- noop
  {%- else -%}
    {% if old_relation is not none %}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
