
{% test column_type(model, column_name, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set validation_message = 'Got a column type of ' ~ col_types.get(column_name) ~ ', expected ' ~ type %}

    {% set val = 0 if col_types.get(column_name) == type else 1 %}
    {% if val == 1 and execute %}
        {{ log(validation_message, info=True) }}
    {% endif %}

    select '{{ validation_message }}' as validation_error
    from (select true) as nothing
    where {{ val }} = 1

{% endtest %}
