
{% macro test_array_results() %}

    {% set sql %}
        select ARRAY[1, 2, 3, 4] as mydata
    {% endset %}

    {% set result = run_query(sql) %}
    {% set value = result.columns['mydata'][0] %}

    {# This will be json-stringified #}
    {% if value != "[1, 2, 3, 4]" %}
        {% do exceptions.raise_compiler_error("Value was " ~ value) %}
    {% endif %}

{% endmacro %}
