{% materialization table, adapter='snowflake' %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set backup_identifier = identifier + '__dbt_backup' -%}
  {%- set non_destructive_mode = config.get("non_destructive", default=False) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set invalid_non_destructive_msg -%}
    Invalid value provided for non_destructive: {{ non_destructive_mode }}
    Expected one of: True, False
  {%- endset %}
  {% if non_destructive_mode not in [True, False] %}
    {% do exceptions.raise_compiler_error(invalid_non_destructive_msg) %}
  {% endif %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database, type='table') -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                      schema=schema,
                                                      database=database, type='table') -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  
  -- Drop the relation if it was a view to "convert" it in a table. This may lead to 
  -- downtime, but it should be a relatively infrequent occurrence 
  {% if exists_as_view %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  {%- set non_destructive = non_destructive_mode and not full_refresh_mode -%}
  {%- set table_swap = exists_as_table and not non_destructive -%}

  --build model
  {%- call statement('main') -%}
    {% if table_swap -%}
      {{ create_table_as(false, intermediate_relation, sql) }}
    
    {% elif non_destructive -%}
      --Commenting out the do truncate command as it forces a commit. Using DELETE instead.
      {# {% do adapter.truncate_relation(old_relation) %} #}
      delete from {{ old_relation.include(database=True, schema=True) }};

      {% set dest_columns = adapter.get_columns_in_relation(old_relation) %}
      {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

      insert into {{ target_relation }} ({{ dest_cols_csv }}) (
        {{ sql }}
      ); 

    {%- else %} 
      {{ create_table_as(false, target_relation, sql) }}

    {%- endif %}
  {%- endcall -%}

  {% if table_swap -%}
    {{ adapter.rename_relation(target_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
