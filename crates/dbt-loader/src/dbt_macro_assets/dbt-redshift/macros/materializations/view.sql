{# FIXME: The macros in this file diverge from v1, https://github.com/dbt-labs/fs/issues/11679 #}
{%- macro redshift__create_or_replace_view_as(relation, sql) -%}
  {%- set binding = config.get('bind', default=True) -%}
  {%- set bind_qualifier = '' if binding else 'with no schema binding' -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}
  create or replace view {{ relation }} as (
    {{ sql }}
  ) {{ bind_qualifier }};
{%- endmacro -%}


{%- materialization view, adapter='redshift', supported_languages=['sql'] -%}
  {# Use CREATE OR REPLACE VIEW to avoid the safe-swap rename+drop pattern which
     causes CASCADE drops to silently remove dependent views on Redshift. #}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    {{ redshift__create_or_replace_view_as(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
