{% materialization table, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}

  {%- set target_relation = this.incorporate(type='table') %}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  {# DIVERGENCE: adapter.table_format is Fusion-only (see issue #10659). Under dbt-core
     (1.x) there is no iceberg table-format routing, so use the standard temp+rename flow. #}
  {% set use_direct_create = (adapter.table_format(target_relation) == 'iceberg') if dbt_version.startswith('2.') else false %}
  {%- set existing_relation = none if use_direct_create else load_cached_relation(this) -%}
  {%- set intermediate_relation = target_relation if use_direct_create else make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = none if use_direct_create else load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = none if use_direct_create else make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = none if use_direct_create else load_cached_relation(backup_relation) -%}

  {% if use_direct_create %}
    {# Iceberg REST catalogs do not support the temp-table + rename flow. #}
    {{ adapter.drop_relation(target_relation) }}
  {% else %}
    -- the intermediate_relation should not already exist in the database; get_relation
    -- will return None in that case. Otherwise, we get a relation that we can drop
    -- later, before we try to use this name for the current operation
    -- as above, the backup_relation should not already exist
    -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main', language=language) -%}
    {{- create_table_as(False, intermediate_relation, compiled_code, language) }}
  {%- endcall %}

  {% if not use_direct_create and existing_relation is not none %}
      {#-- Drop indexes before renaming to avoid dependency errors --#}
      {% do drop_indexes_on_relation(existing_relation) %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {% if not use_direct_create %}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% endif %}

  {% if not use_direct_create %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {% if not use_direct_create %}
    -- finally, drop the existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
