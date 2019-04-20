
{% materialization incremental, adapter='snowflake' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set sql_where = config.get('sql_where') -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set target_relation = api.Relation.create(database=database, identifier=identifier, schema=schema, type='table') -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  -- FIXME: Double check the operators syntax as non-destrive always has to be false here
  {%- set should_drop = (full_refresh_mode or exists_not_as_table and not non_destructive_mode) -%}
  {%- set force_create_or_replace = full_refresh_mode -%}


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

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if force_create_or_replace or old_relation is none -%}
    {%- call statement('main') -%}

      {# -- create or replace logic because we're in a full refresh or table is non existant. #}
      {% if old_relation is not none and old_relation.type == 'view' %}
        {# -- I'm preserving one of the old checks here for a view, and to make sure Snowflake doesn't
        -- complain that we're running a replace table on a view. #}
          {{ log("Dropping relation " ~ old_relation ~ " because it is a view and this model is a table.") }}
          {{ adapter.drop_relation(old_relation) }}
      {% endif %}
  
      {# -- now create or replace the table because we're in full-refresh #}
      {{create_or_replace_table_as(target_relation, source_sql)}}
    {%- endcall -%}

  {%- else -%}
    {# -- here is the incremental part #}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {%- call statement('main') -%}
      {%- if unique_key is none -%}
      {# -- if no unique_key is provided run regular insert as Snowflake may complain #}
        insert into {{ target_relation }} ({{ dest_columns }})
        (
          select {{ dest_columns }}
          from {{ source_sql }}
        );
      {%- else -%}
      {# -- use merge if a unique key is provided #}
        {{ get_merge_sql(target_relation, source_sql, unique_key, dest_columns) }}
      {%- endif -%}
    {% endcall %}

  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

{# -- FIXME: There doesn't seem to be any backup relation created here. Need to check whether we 
-- should have one #}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}
