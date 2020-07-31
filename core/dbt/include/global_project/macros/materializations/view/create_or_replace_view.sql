
{% macro handle_existing_table(full_refresh, old_relation) %}
    {{ adapter.dispatch("handle_existing_table", packages=['dbt'])(full_refresh, old_relation) }}
{% endmacro %}

{% macro default__handle_existing_table(full_refresh, old_relation) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{# /*
       Core materialization implementation. BigQuery and Snowflake are similar
       because both can use `create or replace view` where the resulting view schema
       is not necessarily the same as the existing view. On Redshift, this would
       result in: ERROR:  cannot change number of columns in view

       This implementation is superior to the create_temp, swap_with_existing, drop_old
       paradigm because transactions don't run DDL queries atomically on Snowflake. By using
       `create or replace view`, the materialization becomes atomic in nature.
    */
#}

{% macro create_or_replace_view(run_outside_transaction_hooks=True) %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='view') -%}

  {% if run_outside_transaction_hooks %}
      -- no transactions on BigQuery
      {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {% endif %}

  -- `BEGIN` happens here on Snowflake
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- If there's a table with the same name and we weren't told to full refresh,
  -- that's an error. If we were told to full refresh, drop it. This behavior differs
  -- for Snowflake and BigQuery, so multiple dispatch is used.
  {%- if old_relation is not none and old_relation.is_table -%}
    {{ handle_existing_table(should_full_refresh(), old_relation) }}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if run_outside_transaction_hooks %}
      -- No transactions on BigQuery
      {{ run_hooks(post_hooks, inside_transaction=False) }}
  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}
