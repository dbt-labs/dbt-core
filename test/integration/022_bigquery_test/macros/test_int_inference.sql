
{% macro test_int_inference() %}

    {% set sql %}
        select
            0 as int_0,
            1 as int_1,
            2 as int_2
    {% endset %}

    {% do return(run_query(sql)) %}

{% endmacro %}
