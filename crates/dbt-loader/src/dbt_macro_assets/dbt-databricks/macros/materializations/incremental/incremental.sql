{% materialization incremental, adapter='databricks', supported_languages=['sql', 'python'] -%}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'merge' -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set tblproperties = config.get('tblproperties') -%}
  {%- set tags = config.get('databricks_tags') -%}
  {%- set unique_tmp_table_suffix = config.get('unique_tmp_table_suffix', False) | as_bool -%}

  {%- set file_format = dbt_databricks_validate_get_file_format(raw_file_format) -%}
  {%- set incremental_strategy = dbt_databricks_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set incremental_predicates = config.get('predicates', default=none) or config.get('incremental_predicates', default=none) -%}
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier, needs_information=True) -%}
  {%- if unique_tmp_table_suffix == True and raw_strategy in ['merge', 'replace_where'] and raw_file_format == 'delta' -%}
    {%- set temp_relation_suffix = adapter.generate_unique_temporary_table_suffix() -%}
  {%- else -%}
    {%- set temp_relation_suffix = '__dbt_tmp' -%}
  {%- endif -%}

  {#-- Set Overwrite Mode to STATIC for initial replace --#}
  {%- if incremental_strategy == 'insert_overwrite' and should_full_refresh() -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = STATIC
    {%- endcall -%}
  {%- endif -%}

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
    {% set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}
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
      {%- call statement() -%}
        set spark.sql.sources.partitionOverwriteMode = DYNAMIC
      {%- endcall -%}
    {%- endif -%}
    {#-- Relation must be merged --#}
    {%- set _existing_config = adapter.get_relation_config(existing_relation) -%}
    {%- set model_config = adapter.get_config_from_model(config.model) -%}
    {%- set _configuration_changes = model_config.get_changeset(_existing_config) -%}
    {%- set temp_relation = databricks__make_temp_relation(target_relation, suffix=temp_relation_suffix, as_table=language != 'sql') -%}
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
    {% do apply_liquid_clustered_cols(target_relation) %}
    {% if _configuration_changes is not none %}
      {% set tags = _configuration_changes.changes.get("tags", None) %}
      {% set tblproperties = _configuration_changes.changes.get("tblproperties", None) %}
      {% if tags is not none %}
        {% do apply_tags(target_relation, tags.set_tags, tags.unset_tags) %}
      {%- endif -%}
      {% if tblproperties is not none %}
        {% do apply_tblproperties(target_relation, tblproperties.tblproperties) %}
      {%- endif -%}
    {%- endif -%}
    {% do persist_docs(target_relation, model, for_relation=True) %}
  {%- endif -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% do optimize(target_relation) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
