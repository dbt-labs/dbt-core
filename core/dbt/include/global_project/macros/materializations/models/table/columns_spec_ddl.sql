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

{#
  Compares the column schema provided by a model's sql file to the column schema provided by a model's schema file.
  If any differences in name, data_type or order of columns exist between the two schemas, raises a compiler error
#}
{% macro assert_columns_equivalent(sql) %}
  {#-- Obtain the column schema provided by sql file. #}
  {%- set sql_file_provided_columns = get_column_schema_from_query(sql) -%}
  {#--Obtain the column schema provided by the schema file by generating an 'empty schema' query from the model's columns. #}
  {%- set schema_file_provided_columns = get_column_schema_from_query(get_empty_schema_sql(model['columns'])) -%}

  {#-- For compiler error msg #}
  {%- set sql_columns = (format_columns(sql_file_provided_columns)|trim)  -%}
  {%- set yaml_columns = (format_columns(schema_file_provided_columns)|trim)  -%}

  {%- if sql_file_provided_columns|length != schema_file_provided_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- for sql_col in sql_file_provided_columns -%}
    {%- set yaml_col = [] -%}
    {%- for schema_col in schema_file_provided_columns -%}
      {%- if schema_col.name == sql_col.name -%}
        {%- do yaml_col.append(schema_col) -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not yaml_col -%}
      {#-- Column with name not found in yaml --#}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
    {%- if sql_col.dtype != yaml_col[0].dtype -%}
      {#-- Column data types don't match --#}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
  {%- endfor -%}

{% endmacro %}

{% macro format_columns(columns) %}
  {% set formatted_columns = [] %}
  {% for column in columns %}
    {%- set formatted_column = adapter.dispatch('format_column', 'dbt')(column) -%}
    {%- do formatted_columns.append(formatted_column) -%}
  {% endfor %}
  {{ return(formatted_columns|join(', ')) }}
{%- endmacro -%}

{% macro default__format_column(column) -%}
  {{ return(column.column.lower() ~ " " ~ column.dtype) }}
{%- endmacro -%}
