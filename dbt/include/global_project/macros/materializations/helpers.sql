{% macro run_hooks(hooks) %}
  {% for hook in hooks %}
    {% statement %}
      {{ hook }};
    {% endstatement %}
  {% endfor %}
{% endmacro %}

{% macro column_list(columns) %}
  {%- for col in columns %}
    "{{ col.name }}" {% if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}
