
{% materialization incremental, adapter='snowflake' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(database=database,
                                                schema=schema,
                                                identifier=identifier,
                                                type='table') -%}

  {%- set tmp_relation = make_temp_relation(target_relation) %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# -- If the destination is a view, then we have no choice but to drop it #}
  {% if old_relation is not none and old_relation.type == 'view' %}
      {{ log("Dropping relation " ~ old_relation ~ " because it is a view and this model is a table.") }}
      {{ adapter.drop_relation(old_relation) }}
      {% set old_relation = none %}
  {% endif %}

  -- build model
  {% if full_refresh_mode or old_relation is none -%}

    {%- call statement('main') -%}
      {{ create_table_as(false, target_relation, sql) }}
    {%- endcall -%}

  {%- else -%}

    {%- call statement() -%}
       {{ create_table_as(true, tmp_relation, sql) }}
    {%- endcall -%}

    {{ adapter.expand_target_column_types(from_relation=tmp_relation,
                                          to_relation=target_relation) }}
    {% set incremental_sql %}
    (
        select * from {{ tmp_relation }}
    )
    {% endset %}

    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {%- call statement('main') -%}
      {{ get_merge_sql(target_relation, incremental_sql, unique_key, dest_columns) }}
    {% endcall %}

  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}
