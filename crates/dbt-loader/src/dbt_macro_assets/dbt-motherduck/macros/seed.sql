{% macro motherduck__get_binding_char() %}
  {{ return('%s') }}
{% endmacro %}

{% macro motherduck__get_batch_size() %}
  {{ return(500) }}
{% endmacro %}

{% macro motherduck__load_csv_rows(model, agate_table) %}
  {{ return(default__load_csv_rows(model, agate_table)) }}
{% endmacro %}
