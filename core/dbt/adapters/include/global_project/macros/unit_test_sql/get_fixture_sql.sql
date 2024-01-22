{% macro get_fixture_sql(rows, column_name_to_data_types) %}
-- Fixture for {{ model.name }}
{% set default_row = {} %}

{%- if not column_name_to_data_types -%}
{%-   set columns_in_relation = adapter.get_columns_in_relation(this) -%}
{%-   set column_name_to_data_types = {} -%}
{%-   for column in columns_in_relation -%}
{%-     do column_name_to_data_types.update({column.name: column.dtype}) -%}
{%-   endfor -%}
{%- endif -%}

{%- if not column_name_to_data_types -%}
    {{ exceptions.raise_compiler_error("Not able to get columns for unit test '" ~ model.name ~ "' from relation " ~ this) }}
{%- endif -%}

{%- for column_name, column_type in column_name_to_data_types.items() -%}
    {%- do default_row.update({column_name: (safe_cast("null", column_type) | trim )}) -%}
{%- endfor -%}

{%- for row in rows -%}
{%-   do format_row(row, column_name_to_data_types) -%}
{%-   set default_row_copy = default_row.copy() -%}
{%-   do default_row_copy.update(row) -%}
select
{%-   for column_name, column_value in default_row_copy.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%}, {%- endif %}
{%-   endfor %}
{%-   if not loop.last %}
union all
{%    endif %}
{%- endfor -%}

{%- if (rows | length) == 0 -%}
    select
    {%- for column_name, column_value in default_row.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%},{%- endif %}
    {%- endfor %}
    limit 0
{%- endif -%}
{% endmacro %}


{% macro get_expected_sql(rows, column_name_to_data_types) %}

{%- if (rows | length) == 0 -%}
    select * FROM dbt_internal_unit_test_actual
    limit 0
{%- else -%}
{%- for row in rows -%}
{%- do format_row(row, column_name_to_data_types) -%}
select
{%- for column_name, column_value in row.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%}, {%- endif %}
{%- endfor %}
{%- if not loop.last %}
union all
{% endif %}
{%- endfor -%}
{%- endif -%}

{% endmacro %}

{%- macro format_row(row, column_name_to_data_types) -%}

{#-- wrap yaml strings in quotes, apply cast --#}
{%- for column_name, column_value in row.items() -%}
{% set row_update = {column_name: column_value} %}
{%- if column_value is string -%}
{%- set row_update = {column_name: safe_cast(dbt.string_literal(column_value), column_name_to_data_types[column_name]) } -%}
{%- elif column_value is none -%}
{%- set row_update = {column_name: safe_cast('null', column_name_to_data_types[column_name]) } -%}
{%- else -%}
{%- set row_update = {column_name: safe_cast(column_value, column_name_to_data_types[column_name]) } -%}
{%- endif -%}
{%- do row.update(row_update) -%}
{%- endfor -%}

{%- endmacro -%}
