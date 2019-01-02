
{% macro test_column_type(model, field, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% set _ = col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set val = 0 if col_types[field] == type else 1 %}

    select {{ val }} as pass_fail

{% endmacro %}
