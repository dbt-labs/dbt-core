{% macro get_create_sql_refresh_schedule(cron, time_zone_value) %}
  {%- if cron -%}
    SCHEDULE CRON '{{ cron }}'{%- if time_zone_value %} AT TIME ZONE '{{ time_zone_value }}'{%- endif -%}
  {%- endif -%}
{% endmacro %}

{% macro get_alter_sql_refresh_schedule(cron, time_zone_value, is_altered) %}
  {%- if cron -%}
    {%- if is_altered -%}
      ALTER SCHEDULE CRON '{{ cron }}'{%- if time_zone_value %} AT TIME ZONE '{{ time_zone_value }}'{%- endif -%}
    {%- else -%}
      ADD SCHEDULE CRON '{{ cron }}'{%- if time_zone_value %} AT TIME ZONE '{{ time_zone_value }}'{%- endif -%}
    {%- endif -%}
  {%- else -%}
    DROP SCHEDULE
  {%- endif -%}
{% endmacro %}
