{% set result = adapter.get_columns_in_relation(this) %}
{% if execute and result != 'a string' %}
  {% do exceptions.raise_compiler_error('overriding get_columns_in_relation failed') %}
{% endif %}
select 1 as id
