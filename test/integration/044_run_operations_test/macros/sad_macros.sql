{% macro syntax_error() %}
  {% if execute %}
    {% call statement() %}
        select NOPE NOT A VALID QUERY
    {% endcall %}
  {% endif %}
{% endmacro %}
