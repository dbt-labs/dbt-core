{% macro my_macro(something) %}

    select
        '{{ something }}' as something2

{% endmacro %}
