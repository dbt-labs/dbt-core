
{% macro run_query(sql) %}
  {% call statement("run_query_statement", fetch_result=true, auto_begin=false) %}
    {{ sql }}
  {% endcall %}

  {% do return(load_result("run_query_statement").table) %}
{% endmacro %}
