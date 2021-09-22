
{% macro do_something2(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}


{% macro with_ref() %}

    {{ ref('table_model') }}

{% endmacro %}


{% macro dispatch_to_parent() %}
	{% set macro = adapter.dispatch('dispatch_to_parent') %}
	{{ macro() }}
{% endmacro %}

{% macro default__dispatch_to_parent() %}
	{% set msg = 'No default implementation of dispatch_to_parent' %}
    {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro postgres__dispatch_to_parent() %}
	{{ return('') }}
{% endmacro %}
