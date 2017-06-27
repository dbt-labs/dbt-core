{% macro run_hooks(hooks) %}
  {% for hook in hooks %}
    {% statement %}
      {{ hook }};
    {% endstatement %}
  {% endfor %}
{% endmacro %}
