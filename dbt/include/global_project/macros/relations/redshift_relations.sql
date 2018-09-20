{% macro redshift__get_relations () -%}
  {# TODO: is this allowed? #}
  {{ return(dbt.postgres__get_relations()) }}
{% endmacro %}
