{% materialization incremental, adapter='databricks', supported_languages=['sql', 'python'] -%}
  {{ log("MATERIALIZING INCREMENTAL") }}
  {# -- todo: use adapter.build_catalog_relation here once we have support for it -- #}

  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'merge' -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set tblproperties = config.get('tblproperties') -%}
  {%- set tags = config.get('databricks_tags') -%}

  {%- set file_format = dbt_databricks_validate_get_file_format(raw_file_format) -%}
  {%- set incremental_strategy = dbt_databricks_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set full_refresh = should_full_refresh() %}
  {%- set incremental_predicates = config.get('predicates', default=none) or config.get('incremental_predicates', default=none) -%}
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation_with_metadata(this) %}
  {%- set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}

  {% if adapter.behavior.use_materialization_v2 %}
    {{ log("USING V2 MATERIALIZATION") }}
    {#-- Set vars --#}
    {% set safe_create = config.get('use_safer_relation_operations', False) | as_bool  %}
    {{ log("Safe create: " ~ safe_create) }}
    {% set should_replace = existing_relation.is_dlt or existing_relation.is_view or full_refresh %}
    {% set is_replaceable = existing_relation.can_be_replaced and is_delta and config.get("location_root") %}

    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% set staging_relation = make_staging_relation(target_relation) %}

    {{ run_pre_hooks() }}

    {% call statement('main', language=language) %}
      {{ get_create_intermediate_table(intermediate_relation, compiled_code, language) }}
    {% endcall %}

    {#-- Incremental run logic --#}
    {%- if existing_relation is none -%}
      {{ log("No existing relation found") }}
      {{ create_table_at(target_relation, intermediate_relation, compiled_code) }}
    {%- elif should_replace -%}
      {{ log("Existing relation found that requires replacement") }}
      {% if safe_create and existing_relation.can_be_renamed %}
        {{ log("Safe create enabled and relation can be renamed") }}
        {{ safe_relation_replace(existing_relation, staging_relation, intermediate_relation, compiled_code) }}
      {% else %}
        {#-- Relation must be dropped & recreated --#}
        {% if not is_replaceable %} {#-- If Delta, we will `create or replace` below, so no need to drop --#}
          {{ log("Dropping existing relation, as it is not replaceable") }}
          {% do adapter.drop_relation(existing_relation) %}
        {% endif %}
        {{ log("Replacing target relation") }}
        {{ create_table_at(target_relation, intermediate_relation, compiled_code) }}
      {% endif %}
    {%- else -%}
      {{ log("Existing relation found, proceeding with incremental work")}}
      {#-- Set Overwrite Mode to DYNAMIC for subsequent incremental operations --#}
      {%- if incremental_strategy == 'insert_overwrite' and partition_by -%}
        {{ set_overwrite_mode('DYNAMIC') }}
      {%- endif -%}
      {#-- Relation must be merged --#}
      {%- do process_schema_changes(on_schema_change, intermediate_relation, existing_relation) -%}
      {{ process_config_changes(target_relation) }}
      {% set build_sql = get_build_sql(incremental_strategy, target_relation, intermediate_relation) %}
      {%- if language == 'sql' -%}
        {%- call statement('main') -%}
          {{ build_sql }}
        {%- endcall -%}
      {%- elif language == 'python' -%}
        {%- call statement_with_staging_table('main', intermediate_relation) -%}
          {{ build_sql }}
        {%- endcall -%}
      {%- endif -%}
    {%- endif -%}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
    {% do apply_grants(target_relation, grant_config, should_revoke) %}
    {% do optimize(target_relation) %}

    {% if language == 'python' %}
      {{ drop_relation_if_exists(intermediate_relation) }}
    {% endif %}

    {{ run_post_hooks() }}

  {% else %}
    {% set temp_relation = make_temp_relation(target_relation) %}
    {#-- Run pre-hooks --#}
    {{ run_hooks(pre_hooks) }}
    {#-- Incremental run logic --#}
    {%- if existing_relation is none -%}
      {#-- Relation must be created --#}
      {%- call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
      {%- endcall -%}
      {% do persist_constraints(target_relation, model) %}
      {% do apply_tags(target_relation, tags) %}
      {%- if language == 'python' -%}
        {%- do apply_tblproperties(target_relation, tblproperties) %}
      {%- endif -%}

      {% do persist_docs(target_relation, model, for_relation=language=='python') %}
    {%- elif existing_relation.is_view or existing_relation.is_materialized_view or existing_relation.is_streaming_table or should_full_refresh() -%}
      {#-- Relation must be dropped & recreated --#}
      {% if not is_delta %} {#-- If Delta, we will `create or replace` below, so no need to drop --#}
        {% do adapter.drop_relation(existing_relation) %}
      {% endif %}
      {%- call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
      {%- endcall -%}

      {% if not existing_relation.is_view %}
        {% do persist_constraints(target_relation, model) %}
      {% endif %}
      {% do apply_tags(target_relation, tags) %}
      {% do persist_docs(target_relation, model, for_relation=language=='python') %}
    {%- else -%}
      {#-- Set Overwrite Mode to DYNAMIC for subsequent incremental operations --#}
      {%- if incremental_strategy == 'insert_overwrite' and partition_by -%}
        {{ set_overwrite_mode('DYNAMIC') }}
      {%- endif -%}
      {#-- Relation must be merged --#}
      {%- set _existing_config = adapter.get_relation_config(existing_relation) -%}
      {%- set model_config = adapter.get_config_from_model(config.model) -%}
      {%- set _configuration_changes = model_config.get_changeset(_existing_config) -%}
      {%- call statement('create_temp_relation', language=language) -%}
        {{ create_table_as(True, temp_relation, compiled_code, language) }}
      {%- endcall -%}
      {%- do process_schema_changes(on_schema_change, temp_relation, existing_relation) -%}
      {%- set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) -%}
      {%- set strategy_arg_dict = ({
              'target_relation': target_relation,
              'temp_relation': temp_relation,
              'unique_key': unique_key,
              'dest_columns': none,
              'incremental_predicates': incremental_predicates}) -%}
      {%- set build_sql = strategy_sql_macro_func(strategy_arg_dict) -%}
      {%- if language == 'sql' -%}
        {%- call statement('main') -%}
          {{ build_sql }}
        {%- endcall -%}
      {%- elif language == 'python' -%}
        {%- call statement_with_staging_table('main', temp_relation) -%}
          {{ build_sql }}
        {%- endcall -%}
        {#--
        This is yucky.
        See note in dbt-spark/dbt/include/spark/macros/adapters.sql
        re: python models and temporary views.

        Also, why does not either drop_relation or adapter.drop_relation work here?!
        --#}
      {%- endif -%}
      {% if _configuration_changes is not none %} 
        {% set tags = _configuration_changes.changes.get("tags", None) %}
        {% set tblproperties = _configuration_changes.changes.get("tblproperties", None) %}
        {% set liquid_clustering = _configuration_changes.changes.get("liquid_clustering") %}
        {% if tags is not none %}
          {% do apply_tags(target_relation, tags.set_tags) %}
        {%- endif -%}
        {% if tblproperties is not none %}
          {% do apply_tblproperties(target_relation, tblproperties.tblproperties) %}
        {%- endif -%}
        {% if liquid_clustering is not none %}
          {% do apply_liquid_clustered_cols(target_relation, liquid_clustering) %}
        {% endif %}
      {%- endif -%}
      {% do persist_docs(target_relation, model, for_relation=True) %}
    {%- endif -%}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
    {% do apply_grants(target_relation, grant_config, should_revoke) %}
    {% do optimize(target_relation) %}

    {{ run_hooks(post_hooks) }}
    -- This is intentional - it's to create a view relation instead of a temp view
    -- since DBX v2 api doesn't support session
    {% if temp_relation %}
      {% do adapter.drop_relation(temp_relation) %}
    {% endif %}
  {%- endif %}

  {%- if incremental_strategy == 'insert_overwrite' and not full_refresh -%}
    {{ set_overwrite_mode('STATIC') }}
  {%- endif -%}
  
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro set_overwrite_mode(value) %}
  {% if adapter.is_cluster() %}
    {%- call statement('Setting partitionOverwriteMode: ' ~ value) -%}
      set spark.sql.sources.partitionOverwriteMode = {{ value }}
    {%- endcall -%}
  {% else %}
    {{ exceptions.warn("INSERT OVERWRITE is only properly supported on all-purpose clusters.  On SQL Warehouses, this strategy would be equivalent to using the table materialization.") }}
  {% endif %}
{% endmacro %}

{% macro get_build_sql(incremental_strategy, target_relation, intermediate_relation) %}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set incremental_predicates = config.get('predicates') or config.get('incremental_predicates') -%}
  {%- set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) -%}
  {%- set strategy_arg_dict = ({
          'target_relation': target_relation,
          'temp_relation': intermediate_relation,
          'unique_key': unique_key,
          'dest_columns': none,
          'incremental_predicates': incremental_predicates}) -%}
  {{ strategy_sql_macro_func(strategy_arg_dict) }}
{% endmacro %}

{% macro process_config_changes(target_relation) %}
  {% set apply_config_changes = config.get('incremental_apply_config_changes', True) | as_bool %}
  {% if apply_config_changes %}
    {%- set existing_config = adapter.get_relation_config(target_relation) -%}
    {%- set model_config = adapter.get_config_from_model(config.model) -%}
    {%- set configuration_changes = model_config.get_changeset(existing_config) -%}
    {{ apply_config_changeset(target_relation, model, configuration_changes) }}
  {% endif %}
{% endmacro %}