{% macro databricks__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}


{%- materialization clone, adapter='databricks' -%}

  {%- set relations = {'relations': []} -%}

  {%- if not defer_relation -%}
      -- nothing to do
      {{ log("No relation found in state manifest for " ~ model.unique_id, info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- if existing_relation and not flags.FULL_REFRESH -%}
      -- noop!
      {{ log("Relation " ~ existing_relation ~ " already exists", info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set other_existing_relation = load_cached_relation(defer_relation) -%}
  {%- set file_format = adapter.resolve_file_format(config) -%}

  -- If this is a database that can do zero-copy cloning of tables, and the other relation is a table, then this will be a table
  -- Otherwise, this will be a view

  {% set can_clone_table = can_clone_table() %}

  {# DIVERGENCE BEGIN: relation.type is an Object in Fusion; upstream uses StrEnum. Use '.is_table' instead of `== 'table'`#}
  {%- if other_existing_relation and other_existing_relation.is_table and can_clone_table -%}
  {# DIVERGENCE END #}

      {%- set target_relation = this.incorporate(type='table') -%}
      {% if existing_relation is not none and not existing_relation.is_table %}
        {{ log("Dropping relation " ~ existing_relation ~ " because it is of type " ~ existing_relation.type) }}
        {{ drop_relation_if_exists(existing_relation) }}
      {% endif %}

      -- as a general rule, data platforms that can clone tables can also do atomic 'create or replace'
      {% if other_existing_relation.is_external_table %}
          {% call statement('main') %}
              {{ create_or_replace_clone_external(target_relation, defer_relation) }}
          {% endcall %}
      {% else %}
          {% call statement('main') %}
              {{ create_or_replace_clone(target_relation, defer_relation) }}
          {% endcall %}
      {% endif %}

      {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
      {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
      {% do persist_docs(target_relation, model) %}

      {{ return({'relations': [target_relation]}) }}

  {%- else -%}

      {%- set target_relation = this.incorporate(type='view') -%}

      -- reuse the view materialization
      -- TODO: support actual dispatch for materialization macros
      -- Tracking ticket: https://github.com/dbt-labs/dbt-core/issues/7799
      {% set search_name = "materialization_view_" ~ adapter.type() %}
      {% if not search_name in context %}
          {% set search_name = "materialization_view_default" %}
      {% endif %}
      {% set materialization_macro = context[search_name] %}
      {% set relations = materialization_macro() %}
      {{ return(relations) }}
  {% endif %}

{%- endmaterialization -%}
