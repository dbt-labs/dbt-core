
{% materialization incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set sql_where = config.get('sql_where') -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% if non_destructive_mode %}
    {{ exceptions.raise_compiler_error("--non-destructive mode is not supported on BigQuery") }}
  {% endif %}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set target_relation = api.Relation.create(database=database, identifier=identifier, schema=schema, type='table') -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  {%- set should_drop = (full_refresh_mode or exists_not_as_table) -%}
  {%- set force_create = (full_refresh_mode) -%}

  -- setup
  {% if old_relation is none -%}
    -- noop
  {%- elif should_drop -%}
    {{ adapter.drop_relation(old_relation) }}
    {%- set old_relation = none -%}
  {%- endif %}

  {% set source_sql -%}
     {#-- wrap sql in parens to make it a subquery --#}
     (
        select * from (
            {{ sql }}
        )
        {% if sql_where %}
            where ({{ sql_where }}) or ({{ sql_where }}) is null
        {% endif %}
    )
  {%- endset -%}


  {{ run_hooks(pre_hooks) }}

  -- build model
  {% if force_create or old_relation is none -%}
    {%- call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}
     {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
     {%- call statement('main') -%}
       {{ get_merge_sql(target_relation, source_sql, unique_key, dest_columns) }}
     {% endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks) }}

{%- endmaterialization %}
