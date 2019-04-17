{% macro no_args() %}
  {% if execute %}
    {% call statement() %}
      create table "{{ schema }}"."no_args" (id int)
    {% endcall %}
  {% endif %}
{% endmacro %}


{% macro table_name_args(table_name) %}
  {% if execute %}
    {% call statement() %}
      create table "{{ schema }}"."{{ table_name }}" (id int)
    {% endcall %}
  {% endif %}
{% endmacro %}
