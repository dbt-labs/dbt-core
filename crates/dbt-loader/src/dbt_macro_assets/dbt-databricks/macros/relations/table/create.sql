{% macro databricks__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- if temporary -%}
      -- INTENTIONAL DIVERGENCE 
      -- create_temporary_view method cannot be used here, because DBX v2 api doesn't support session
      {{ _create_view_simple(relation, compiled_code) }}
    {%- else -%}
      {%- set file_format = config.get('file_format', default='delta') -%}
      {% if file_format == 'delta' %}
        create or replace table {{ relation.render() }}
      {% else %}
        create table {{ relation.render() }}
      {% endif %}
      {%- set contract_config = config.get('contract') -%}
      {% if contract_config and contract_config.enforced %}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
      {% endif %}
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ liquid_clustered_cols() }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause(relation) }}
      {{ comment_clause() }}
      {{ tblproperties_clause() }}
      as
      {{ compiled_code }}
    {%- endif -%}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}
    {{ databricks__py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
{%- endmacro -%}

{% macro databricks__options_clause() -%}
  {%- set options = config.get('options') -%}
  {%- if config.get('file_format', default='delta') == 'hudi' -%}
    {%- set unique_key = config.get('unique_key') -%}
    {%- if unique_key is not none and options is none -%}
      {%- set options = {'primaryKey': config.get('unique_key')} -%}
    {%- elif unique_key is not none and options is not none and 'primaryKey' not in options -%}
      {%- set _ = options.update({'primaryKey': config.get('unique_key')}) -%}
    {%- elif options is not none and 'primaryKey' in options and options['primaryKey'] != unique_key -%}
      {{ exceptions.raise_compiler_error("unique_key and options('primaryKey') should be the same column(s).") }}
    {%- endif %}
  {%- endif %}

  {%- if options is not none %}
    options (
      {%- for option in options -%}
      {{ option }} "{{ options[option] }}" {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}


-- INTENTIONAL DIVERGENCE
{% macro _create_view_simple(relation, compiled_code) -%}
    create or replace view {{ relation }} as
      {{ compiled_code }}
{%- endmacro -%}