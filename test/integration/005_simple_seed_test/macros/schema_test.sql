
{% macro test_column_type(model, column_name, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set val = 0 if col_types.get(column_name) == type else 1 %}

    select {{ val }} as pass_fail

{% endmacro %}
