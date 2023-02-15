{%- macro get_columns_spec_ddl() -%}
  {{ adapter.dispatch('get_columns_spec_ddl', 'dbt')() }}
{%- endmacro -%}

{% macro default__get_columns_spec_ddl() -%}
  {{ return(columns_spec_ddl()) }}
{%- endmacro %}

{% macro columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {% set constraints = col['constraints'] %}
      {% set constraints_check = col['constraints_check'] %}
      {{ col['name'] }} {{ col['data_type'] }} {% for x in constraints %} {{ x or "" }} {% endfor %} {% if constraints_check -%} check {{ constraints_check or "" }} {%- endif %} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}

{%- macro get_assert_columns_equivalent(sql) -%}
  {{ adapter.dispatch('get_assert_columns_equivalent', 'dbt')(sql) }}
{%- endmacro -%}

{% macro default__get_assert_columns_equivalent(sql) -%}
  {{ return(assert_columns_equivalent(sql)) }}
{%- endmacro %}

{% macro assert_columns_equivalent(sql) %}
  {#- loop through user_provided_columns to get column names -#}
    {%- set user_provided_columns = model['columns'] -%}
    {%- set column_names_config_only = [] -%}
    {%- for i in user_provided_columns -%}
      {%- set col = user_provided_columns[i] -%}
      {%- set col_name = col['name'] -%}
      {%- set column_names_config_only = column_names_config_only.append(col_name) -%}
    {%- endfor -%}
    {%- set sql_file_provided_columns = get_columns_in_query(sql) -%}

    {#- uppercase both schema and sql file columns -#}
    {%- set column_names_config_upper= column_names_config_only|map('upper')|join(',')  -%}
    {%- set column_names_config_formatted = column_names_config_upper.split(',')  -%}
    {%- set sql_file_provided_columns_upper = sql_file_provided_columns|map('upper')|join(',') -%}
    {%- set sql_file_provided_columns_formatted = sql_file_provided_columns_upper.split(',') -%}

    {%- if column_names_config_formatted != sql_file_provided_columns_formatted -%}
      {%- do exceptions.raise_compiler_error('Please ensure the name, order, and number of columns in your `yml` file match the columns in your SQL file.\nSchema File Columns: ' ~ column_names_config_formatted ~ '\nSQL File Columns: ' ~ sql_file_provided_columns_formatted ~ ' ' ) %}
    {%- endif -%}

{% endmacro %}
