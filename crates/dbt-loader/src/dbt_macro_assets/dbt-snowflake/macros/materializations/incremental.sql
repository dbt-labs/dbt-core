-- funcsign: (strategy, optional[string], string) -> string
{% macro dbt_snowflake_get_tmp_relation_type(strategy, unique_key, language) %}
{%- set tmp_relation_type = config.get('tmp_relation_type') -%}
  /* {#
       High-level principles:
       If we are running multiple statements (DELETE + INSERT),
       and we want to guarantee identical inputs to both statements,
       then we must first save the model query results as a temporary table
       (which presumably comes with a performance cost).
       If we are running a single statement (MERGE or INSERT alone),
       we _may_ save the model query definition as a view instead,
       for (presumably) faster overall incremental processing.

       Low-level specifics:
       If an invalid option is specified, then we will raise an
       excpetion with corresponding message.

       Languages other than SQL (like Python) will use a temporary table.
       With the default strategy of merge, the user may choose between a temporary
       table and view (defaulting to view).

       The append strategy can use a view because it will run a single INSERT statement.

       When unique_key is none, the delete+insert and microbatch strategies can use a view beacuse a
       single INSERT statement is run with no DELETES as part of the statement.
       Otherwise, play it safe by using a temporary table.
  #} */

  {% if language == "python" and tmp_relation_type is not none %}
    {% do exceptions.raise_compiler_error(
      "Python models currently only support 'table' for tmp_relation_type but "
       ~ tmp_relation_type ~ " was specified."
    ) %}
  {% endif %}

  {% if strategy in ["delete+insert", "microbatch"] and tmp_relation_type is not none and tmp_relation_type != "table" and unique_key is not none %}
    {% do exceptions.raise_compiler_error(
      "In order to maintain consistent results when `unique_key` is not none,
      the `" ~ strategy ~ "` strategy only supports `table` for `tmp_relation_type` but "
      ~ tmp_relation_type ~ " was specified."
      )
  %}
  {% endif %}

  {% if language != "sql" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "table" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "view" %}
    {{ return("view") }}
  {% elif strategy in ("default", "merge", "append", "insert_overwrite") %}
    {{ return("view") }}
  {% elif strategy in ["delete+insert", "microbatch"] and unique_key is none %}
    {{ return("view") }}
  {% else %}
    {{ return("table") }}
  {% endif %}
{% endmacro %}

{% materialization incremental, adapter='snowflake', supported_languages=['sql', 'python'] -%}

  {% set original_query_tag = set_query_tag() %}

  {#-- Set vars --#}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] -%}

  {%- set identifier = this.name -%}

  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table',
	table_format=config.get('table_format', 'default')
  ) -%}

  {% set existing_relation = load_relation(this) %}

  {#-- The temp relation will be a view (faster) or temp table, depending on upsert/merge strategy --#}
  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, language) %}
  {% set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_type) %}

  {% set grant_config = config.get('grants') %}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif full_refresh_mode %}
    {% if target_relation.needs_to_drop(existing_relation) %}
      {{ drop_relation_if_exists(existing_relation) }}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif target_relation.table_format != existing_relation.table_format %}
    {% do exceptions.raise_compiler_error(
        "Unable to update the incremental model `" ~ target_relation.identifier ~ "` from `" ~ existing_relation.table_format ~ "` to `" ~ target_relation.table_format ~ "` due to Snowflake limitation. Please execute with --full-refresh to drop the table and recreate in the new catalog.'"
      )
    %}

  {% else %}
    {#-- Create the temp relation, either as a view or as a temp table --#}
    {% if tmp_relation_type == 'view' %}
        {%- call statement('create_tmp_relation') -%}
          {{ snowflake__create_view_as_with_temp_flag(tmp_relation, compiled_code, True) }}
        {%- endcall -%}
    {% else %}
        {%- call statement('create_tmp_relation', language=language) -%}
          {{ create_table_as(True, tmp_relation, compiled_code, language) }}
        {%- endcall -%}
    {% endif %}

    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': tmp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}

    {%- call statement('main') -%}
      {{ strategy_sql_macro_func(strategy_arg_dict) }}
    {%- endcall -%}
  {% endif %}

  {% do drop_relation_if_exists(tmp_relation) %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% set should_revoke =
   should_revoke(existing_relation.is_table, full_refresh_mode) %} -- noqa: existing_relation can be none
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{% macro snowflake__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}
