{% macro can_clone_tables() %}
    {{ return(adapter.dispatch('can_clone_tables', 'dbt')()) }}
{% endmacro %}


{% macro default__can_clone_tables() %}
    {{ return(False) }}
{% endmacro %}


{% macro snowflake__can_clone_tables() %}
    {{ return(True) }}
{% endmacro %}


{% macro get_pointer_sql(to_relation) %}
    {{ return(adapter.dispatch('get_pointer_sql', 'dbt')(to_relation)) }}
{% endmacro %}


{% macro default__get_pointer_sql(to_relation) %}
    {% set pointer_sql %}
        select * from {{ to_relation }}
    {% endset %}
    {{ return(pointer_sql) }}
{% endmacro %}


{% macro get_clone_table_sql(this_relation, state_relation) %}
    {{ return(adapter.dispatch('get_clone_table_sql', 'dbt')(this_relation, state_relation)) }}
{% endmacro %}


{% macro default__get_clone_table_sql(this_relation, state_relation) %}
    create or replace table {{ this_relation }} clone {{ state_relation }}
{% endmacro %}


{% macro snowflake__get_clone_table_sql(this_relation, state_relation) %}
    create or replace
      {{ "transient" if config.get("transient", true) }}
      table {{ this_relation }}
      clone {{ state_relation }}
      {{ "copy grants" if config.get("copy_grants", false) }}
{% endmacro %}


{%- materialization clone, default -%}

  {%- set relations = {'relations': []} -%}

  {%- if not state_relation -%}
      -- nothing to do
      {{ log("No relation found in state manifest for " ~ model.unique_id) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- if existing_relation and not flags.FULL_REFRESH -%}
      -- noop!
      {{ log("Relation " ~ existing_relation ~ " already exists") }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set other_existing_relation = load_cached_relation(state_relation) -%}

  -- If this is a database that can do zero-copy cloning of tables, and the other relation is a table, then this will be a table
  -- Otherwise, this will be a view

  {% set can_clone_tables = can_clone_tables() %}

  {%- if other_existing_relation and other_existing_relation.type == 'table' and can_clone_tables -%}

      {%- set target_relation = this.incorporate(type='table') -%}
      {% if existing_relation is not none and not existing_relation.is_table %}
        {{ log("Dropping relation " ~ existing_relation ~ " because it is of type " ~ existing_relation.type) }}
        {{ drop_relation_if_exists(existing_relation) }}
      {% endif %}

      -- as a general rule, data platforms that can clone tables can also do atomic 'create or replace'
      {% call statement('main') %}
          {{ get_clone_table_sql(target_relation, state_relation) }}
      {% endcall %}

      {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
      {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
      {% do persist_docs(target_relation, model) %}

      {{ return({'relations': [target_relation]}) }}

  {%- else -%}

      {%- set target_relation = this.incorporate(type='view') -%}

      -- TODO: this should probably be illegal
      -- I'm just doing it out of convenience to reuse the 'view' materialization logic
      {%- do context.update({
          'sql': get_pointer_sql(state_relation),
          'compiled_code': get_pointer_sql(state_relation)
      }) -%}

      -- reuse the view materialization
      -- TODO: support actual dispatch for materialization macros
      {% set search_name = "materialization_view_" ~ adapter.type() %}
      {% if not search_name in context %}
          {% set search_name = "materialization_view_default" %}
      {% endif %}
      {% set materialization_macro = context[search_name] %}
      {% set relations = materialization_macro() %}
      {{ return(relations) }}

  {%- endif -%}

{%- endmaterialization -%}
