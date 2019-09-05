{% macro table_options() %}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}

{%- endmacro -%}

{% macro get_relation_comment(persist_docs, model) %}

  {%- if persist_docs is not mapping -%}
    {{ exceptions.raise_compiler_error("Invalid value provided for 'persist_docs'. Expected dict but got value: " ~ raw_persist_docs) }}
  {% endif %}

  {% if persist_docs.get('relation', false) %}
    {{ return((model.description | tojson)[1:-1]) }}
  {%- else -%}
    {{ return(none) }}
  {% endif %}

{% endmacro %}
