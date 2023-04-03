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
      {%- set col = user_provided_columns[i] -%}
      {%- set constraints = col['constraints'] -%}
      {{ col['name'] }} {{ col['data_type'] }}{% for c in constraints %} {{ adapter.render_raw_column_constraint(c) }}{% endfor %}{{ "," if not loop.last }}
    {% endfor -%}
    )
{% endmacro %}

{%- macro get_assert_columns_equivalent(sql) -%}
  {{ adapter.dispatch('get_assert_columns_equivalent', 'dbt')(sql) }}
{%- endmacro -%}

{% macro default__get_assert_columns_equivalent(sql) -%}
  {{ return(assert_columns_equivalent(sql)) }}
{%- endmacro %}

{#
  Compares the column schema provided by a model's sql file to the column schema provided by a model's schema file.
  If any differences in name, data_type or number of columns exist between the two schemas, raises a compiler error
#}
{% macro assert_columns_equivalent(sql) %}
  {#-- Obtain the column schema provided by sql file. #}
  {%- set sql_file_provided_columns = get_column_schema_from_query(sql) -%}
  {#--Obtain the column schema provided by the schema file by generating an 'empty schema' query from the model's columns. #}
  {%- set schema_file_provided_columns = get_column_schema_from_query(get_empty_schema_sql(model['columns'])) -%}

  {#-- create dictionaries with name and formatted data type and strings for exception #}
  {%- set sql_columns = format_columns(sql_file_provided_columns) -%}
  {%- set string_sql_columns = stringify_formatted_columns(sql_columns) -%}
  {%- set yaml_columns = format_columns(schema_file_provided_columns)  -%}
  {%- set string_yaml_columns = stringify_formatted_columns(yaml_columns) -%}

  {%- if sql_columns|length != yaml_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- for sql_col in sql_columns -%}
    {%- set yaml_col = [] -%}
    {%- for this_col in yaml_columns -%}
      {%- if this_col['name'] == sql_col['name'] -%}
        {%- do yaml_col.append(this_col) -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not yaml_col -%}
      {#-- Column with name not found in yaml #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
    {%- if sql_col['formatted'] != yaml_col[0]['formatted'] -%}
      {#-- Column data types don't match #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
  {%- endfor -%}

{% endmacro %}

{% macro format_columns(columns) %}
  {% set formatted_columns = [] %}
  {% for column in columns %}
    {%- set formatted_column = adapter.dispatch('format_column', 'dbt')(column) -%}
    {%- do formatted_columns.append({'name': column.name, 'formatted': formatted_column}) -%}
  {% endfor %}
  {{ return(formatted_columns) }}
{% endmacro %}

{% macro stringify_formatted_columns(formatted_columns) %}
  {% set column_strings = [] %}
  {% for column in formatted_columns %}
     {% do column_strings.append(column['formatted']) %}
  {% endfor %}
  {{ return(column_strings|join(', ')) }}
{% endmacro %}

{% macro default__format_column(column) -%}
  {{ return(column.column.lower() ~ " " ~ column.dtype) }}
{%- endmacro -%}
