{% macro databricks__dateadd(datepart, interval, from_date_or_timestamp) %}
  {%- if adapter.compare_dbr_version(10, 4) >= 0 -%}
    timestampadd({{datepart}}, {{interval}}, {{from_date_or_timestamp}})
  {%- else -%}
    {{ spark__dateadd(datepart, interval, from_date_or_timestamp) }}
  {%- endif -%}
{%- endmacro %}
