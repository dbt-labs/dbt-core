{% materialization table, default %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set backup_identifier = identifier + '__dbt_backup' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set existing_relations = adapter.list_relations(schema=schema) -%}
  {%- set old_relation = adapter.get_relation(relations_list=existing_relations,
                                              schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema, type='table') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, type='table') -%}

  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, type=(old_relation.type or 'table')) -%}

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

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ target_relation }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from {{ intermediate_relation.include(schema=(not create_as_temporary)) }}
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
      -- move the existing relation out of the way
      {# /*
         If this is a _view_ on _snowflake_ AND the contract of the _view_ was broken
         by a change to a parent model, then this can throw when `alter table rename` runs, eg:
               View definition for <relation> declared 1 column(s), but view query produces 2 column(s)

         Instead, drop this view to avoid this potential error. Note: This won't work on Redshift,
         as dropping inside the transaction will lead to a "table dropped by concurrent transaction"
         error. In the future, Snowflake should use `create or replace table` syntax to obviate this code
      */ #}
      {% if adapter.type() == 'snowflake' and old_relation.type == 'view' %}
        {{ log("Dropping relation " ~ old_relation ~ " because it is a view and this model is a table.") }}
        {{ drop_relation_if_exists(old_relation) }}
      {% else %}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
      {% endif %}
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
