{% macro run_hooks(hooks, auto_begin=True) %}
  {% for hook in hooks %}
    {% call statement(auto_begin=auto_begin) %}
      {{ hook }};
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro column_list(columns) %}
  {%- for col in columns %}
    "{{ col.name }}" {% if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}


{% macro column_list_for_create_table(columns) %}
  {%- for col in columns %}
    "{{ col.name }}" {{ col.data_type }} {%- if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}
