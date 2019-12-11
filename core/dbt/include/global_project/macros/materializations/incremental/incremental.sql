
{% materialization incremental, default -%}

  {% set unique_key = config.get('unique_key') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}
  {% set on_schema_change = config.get('on_schema_change') %}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  


  {# -- set the type so our rename / drop uses the correct syntax #}
  {% set backup_type = existing_relation.type | default("table") %}
  {% set backup_relation = make_temp_relation(this, "__dbt_backup").incorporate(type=backup_type) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% if on_schema_change is not none and adapter.target_contains_schema_change(target_relation=target_relation, temp_relation=tmp_relation) -%}
        {% if on_schema_change == 'full_refresh'  -%}
          {% do adapter.rename_relation(target_relation, backup_relation) %}
          {% set build_sql = create_table_as(False, target_relation, sql) %}
          {% do to_drop.append(backup_relation) %}
        {% elif on_schema_change == 'fail' -%}
          {{ exceptions.raise_fail_on_schema_change() }}
        {% endif %}
      {% else %}
        {% do run_query(create_table_as(True, tmp_relation, sql)) %}
        {% do adapter.expand_target_column_types(
              from_relation=tmp_relation,
              to_relation=target_relation) %}
        {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
      {% endif %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
