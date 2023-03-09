{% macro postgres__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {% set constraints = col['constraints'] %}
      {{ col['name'] }} {{ col['data_type'] }} {% for x in constraints %} {{ "check" if x.type == "check" else "not null" if x.type == "not_null" else "unique" if x.type == "unique" else "primary key" if x.type == "primary_key" else "foreign key" if x.type == "foreign key" else ""
 }} {{ x.expression or "" }} {% endfor %} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}

{% macro get_column_names() %}
  {# loop through user_provided_columns to get column names #}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {{ col['name'] }} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}
