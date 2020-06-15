-- Macro to alter a column type
{% macro test_alter_column_type(model_name, column_name, new_column_type) %}
  {% set relation = ref(model_name) %}
  {{ alter_column_type(relation, column_name, new_column_type) }}
{% endmacro %}
