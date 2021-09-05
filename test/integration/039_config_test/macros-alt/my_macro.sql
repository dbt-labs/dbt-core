{% macro do_something2(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}

