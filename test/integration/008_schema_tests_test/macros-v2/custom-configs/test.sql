{% test where(model, column_name) %}
  {{ config(where = "1 = 0") }}
  select * from {{ model }}
{% endtest %}

{% test error_if(model, column_name) %}
  {{ config(error_if = "<= 0", warn_if = "<= 0") }}
  select * from {{ model }}
{% endtest %}


{% test warn_if(model, column_name) %}
  {{ config(warn_if = "<= 0", severity = "WARN") }}
  select * from {{ model }}
{% endtest %}

{% test limit(model, column_name) %}
  {{ config(limit = 0) }}
  select * from {{ model }}
{% endtest %}

{% test fail_calc(model, column_name) %}
  {{ config(fail_calc = "count(*) - count(*)") }}
  select * from {{ model }}
{% endtest %}
