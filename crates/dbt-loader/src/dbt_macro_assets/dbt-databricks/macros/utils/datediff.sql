{% macro databricks__datediff(first_date, second_date, datepart) %}
  {%- if adapter.compare_dbr_version(10, 4) >= 0 -%}
    timestampdiff({{datepart}}, {{date_trunc(datepart, first_date)}}, {{date_trunc(datepart, second_date)}})
  {%- else -%}
    {{ spark__datediff(first_date, second_date, datepart) }}
  {%- endif -%}
{%- endmacro %}
