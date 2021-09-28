
{% macro simple_type_check_column(column, check) %}
    {% if check == 'string' %}
        {{ return(column.is_string()) }}
    {% elif check == 'float' %}
        {{ return(column.is_float()) }}
    {% elif check == 'number' %}
        {{ return(column.is_number()) }}
    {% elif check == 'numeric' %}
        {{ return(column.is_numeric()) }}
    {% elif check == 'integer' %}
        {{ return(column.is_integer()) }}
    {% else %}
        {% do exceptions.raise_compiler_error('invalid type check value: ' ~ check) %}
    {% endif %}
{% endmacro %}

{% macro type_check_column(column, type_checks) %}
    {% set failures = [] %}
    {% for type_check in type_checks %}
        {% if type_check.startswith('not ') %}
            {% if simple_type_check_column(column, type_check[4:]) %}
                {% do log('simple_type_check_column got ', True) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% else %}
            {% if not simple_type_check_column(column, type_check) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if (failures | length) > 0 %}
        {% do log('column ' ~ column.name ~ ' had failures: ' ~ failures, info=True) %}
    {% endif %}
    {% do return((failures | length) == 0) %}
{% endmacro %}

{% test is_type(model, column_map) %}
    {% if not execute %}
        {{ return(None) }}
    {% endif %}
    {% if not column_map %}
        {% do exceptions.raise_compiler_error('test_is_type must have a column name') %}
    {% endif %}
    {% set columns = adapter.get_columns_in_relation(model) %}
    {% if (column_map | length) != (columns | length) %}
        {% set column_map_keys = (column_map | list | string) %}
        {% set column_names = (columns | map(attribute='name') | list | string) %}
        {% do exceptions.raise_compiler_error('did not get all the columns/all columns not specified:\n' ~ column_map_keys ~ '\nvs\n' ~ column_names) %}
    {% endif %}
    {% set bad_columns = [] %}
    {% for column in columns %}
        {% set column_key = (column.name | lower) %}
        {% if column_key in column_map %}
            {% set type_checks = column_map[column_key] %}
            {% if not type_checks %}
                {% do exceptions.raise_compiler_error('no type checks?') %}
            {% endif %}
            {% if not type_check_column(column, type_checks) %}
                {% do bad_columns.append(column.name) %}
            {% endif %}
        {% else %}
            {% do exceptions.raise_compiler_error('column key ' ~ column_key ~ ' not found in ' ~ (column_map | list | string)) %}
        {% endif %}
    {% endfor %}
    {% do log('bad columns: ' ~ bad_columns, info=True) %}
    {% for bad_column in bad_columns %}
      select '{{ bad_column }}' as bad_column
      {{ 'union all' if not loop.last }}
    {% endfor %}
      select * from (select 1 limit 0) as nothing
{% endtest %}
