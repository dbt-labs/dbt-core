{# trigger infinite recursion if not handled #}

{% macro my_infinitely_recursive_macro() %}
  {{ return(adapter.dispatch('my_infinitely_recursive_macro')()) }}
{% endmacro %}

{% macro default__my_infinitely_recursive_macro() %}
    {% if unmet_condition %}
        {{ my_infinitely_recursive_macro() }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}
