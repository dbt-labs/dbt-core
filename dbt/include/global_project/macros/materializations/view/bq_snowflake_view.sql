
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


{% macro impl_view_materialization(error_on_clobber_table=False, run_outside_transaction_hooks=True) %}
  {%- set identifier = model['alias'] -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set old_relation = adapter.get_relation(
      schema=schema, identifier=identifier) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema,
      type='view') -%}

  {%- set should_ignore = non_destructive_mode and exists_as_view %}
  {%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}

  {% if run_outside_transaction_hooks %}
      -- no transactions on BigQuery
      {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {% endif %}

  -- `BEGIN` happens here on Snowflake
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- If there's a table with the same name and we weren't told to full refresh,
  -- that's an error. If we were told to full refresh, drop it.
  {%- if old_relation is not none and old_relation.is_table -%}
    {%- if flags.FULL_REFRESH and not non_destructive_mode -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- elif error_on_clobber_table -%}
      {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {%- endif -%}
  {%- endif -%}

  -- build model
  {% if non_destructive_mode -%}
    {% call noop_statement('main', status="PASS", res=None) -%}
      -- Not running : non-destructive mode
      {{ sql }}
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_view_as(target_relation, sql) }}
    {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {#
      -- Don't commit in non-destructive mode _unless_ there are in-transaction hooks
      -- TODO : Figure out some other way of doing this that isn't as fragile
  #}
  {% if has_transactional_hooks or not should_ignore %}
      {{ adapter.commit() }}
  {% endif %}

  {% if run_outside_transaction_hooks %}
      -- No transactions on BigQuery
      {{ run_hooks(post_hooks, inside_transaction=False) }}
  {% endif %}
{% endmacro %}

{% materialization view, adapter='bigquery' -%}
    {{ impl_view_materialization(error_on_clobber_table=True, run_outside_transaction_hooks=False) }}
{%- endmaterialization %}

{% materialization view, adapter='snowflake' -%}
    {{ impl_view_materialization() }}
{%- endmaterialization %}
