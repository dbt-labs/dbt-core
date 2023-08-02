models__dep_macro = """
{{
    dbt_integration_project.do_something("arg1", "arg2")
}}
"""

models__materialization_macro = """
{{
    materialization_macro()
}}
"""

models__with_undefined_macro = """
{{ dispatch_to_nowhere() }}
select 1 as id
"""

models__local_macro = """
{{
    do_something2("arg1", "arg2")
}}

union all

{{
    test.do_something2("arg3", "arg4")
}}
"""

models__ref_macro = """
select * from {{ with_ref() }}
"""

models__override_get_columns_macros = """
{% set result = adapter.get_columns_in_relation(this) %}
{% if execute and result != 'a string' %}
  {% do exceptions.raise_compiler_error('overriding get_columns_in_relation failed') %}
{% endif %}
select 1 as id
"""

models__deprecated_adapter_macro_model = """
{% if some_macro('foo', 'bar') != 'foobar' %}
  {% do exceptions.raise_compiler_error('invalid foobar') %}
{% endif %}
select 1 as id
"""

#
# Macros
#
macros__my_macros = """
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
"""

macros__named_materialization = """
{% macro materialization_macro() %}
    select 1 as foo
{% endmacro %}
"""

macros__no_default_macros = """
{% macro do_something2(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}


{% macro with_ref() %}

    {{ ref('table_model') }}

{% endmacro %}

{# there is no default__dispatch_to_nowhere! #}
{% macro dispatch_to_nowhere() %}
       {% set macro = adapter.dispatch('dispatch_to_nowhere') %}
       {{ macro() }}
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
"""

macros__override_get_columns_macros = """
{% macro get_columns_in_relation(relation) %}
    {{ return('a string') }}
{% endmacro %}
"""

macros__package_override_get_columns_macros = """
{% macro postgres__get_columns_in_relation(relation) %}
    {{ return('a string') }}
{% endmacro %}
"""

macros__deprecated_adapter_macro = """
{% macro some_macro(arg1, arg2) -%}
    {{ adapter_macro('some_macro', arg1, arg2) }}
{%- endmacro %}
"""

macros__incorrect_dispatch = """
{% macro cowsay() %}
  {{ return(adapter.dispatch('cowsay', 'farm_utils')()) }}
{%- endmacro %}

{% macro default__cowsay() %}
  'moo'
{% endmacro %}
"""

# Note the difference between `test_utils` below and `farm_utils` above
models__incorrect_dispatch = """
select {{ test_utils.cowsay() }} as cowsay
"""

dbt_project__incorrect_dispatch = """
name: 'test_utils'
version: '1.0'
config-version: 2

profile: 'default'

macro-paths: ["macros"]
"""
