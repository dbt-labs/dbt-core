
{% macro assert_eq(value, expected, msg) %}
    {% if value != expected %}
        {% do exceptions.raise_compiler_error(msg ~ value) %}
    {% endif %}
{% endmacro %}


{% macro test_int_inference() %}

    {% set sql %}
        select
            0 as int_0,
            1 as int_1,
            2 as int_2
    {% endset %}

    {% set result = run_query(sql) %}
    {% do assert_eq((result | length), 1, 'expected 1 result, got ') %}
    {% set actual_0 = result[0]['int_0'] %}
    {% set actual_1 = result[0]['int_1'] %}
    {% set actual_2 = result[0]['int_2'] %}

    {% do assert_eq(actual_0, 0, 'expected expected actual_0 to be 0, it was ') %}
    {% do assert_eq((actual_0 | string), '0', 'expected string form of actual_0 to be 0, it was ') %}
    {% do assert_eq((actual_0 * 2), 0, 'expected actual_0 * 2 to be 0, it was ') %} {# not 00 #}

    {% do assert_eq(actual_1, 1, 'expected actual_1 to be 1, it was ') %}
    {% do assert_eq((actual_1 | string), '1', 'expected string form of actual_1 to be 1, it was ') %}
    {% do assert_eq((actual_1 * 2), 2, 'expected actual_1 * 2 to be 2, it was ') %} {# not 11 #}

    {% do assert_eq(actual_2, 2, 'expected actual_2 to be 2, it was ') %}
    {% do assert_eq((actual_2 | string), '2', 'expected string form of actual_2 to be 2, it was ') %}
    {% do assert_eq((actual_2 * 2), 4, 'expected actual_2 * 2 to be 4, it was ') %}  {# not 22 #}

{% endmacro %}
