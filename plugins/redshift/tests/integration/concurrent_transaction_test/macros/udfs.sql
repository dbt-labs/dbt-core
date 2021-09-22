
{% macro create_udfs() %}

CREATE OR REPLACE FUNCTION {{ target.schema }}.f_sleep (x float)
RETURNS bool IMMUTABLE
AS
$$
  from time import sleep
  sleep(x)
  return True
$$ LANGUAGE plpythonu;

{% endmacro %}
