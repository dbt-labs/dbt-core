
{% macro do_something2(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}


{% macro with_ref() %}

    {{ ref('table_model') }}

{% endmacro %}


{# there is no no default__dispatch_to_nowhere! #}
{% macro dispatch_to_nowhere() %}
	{% set macro = adapter.dispatch('dispatch_to_nowhere') %}
	{{ macro() }}
{% endmacro %}
