{% macro no_args() %}
  {% if execute %}
    {% call statement(auto_begin=True) %}
      create table "{{ schema }}"."no_args" (id int);
      commit;
    {% endcall %}
  {% endif %}
{% endmacro %}


{% macro table_name_args(table_name) %}
  {% if execute %}
    {% call statement(auto_begin=True) %}
      create table "{{ schema }}"."{{ table_name }}" (id int);
      commit;
    {% endcall %}
  {% endif %}
{% endmacro %}

{% macro vacuum(table_name) %}
  {% call statement(auto_begin=false) %}
    vacuum "{{ schema }}"."{{ table_name }}"
  {% endcall %}
{% endmacro %}
