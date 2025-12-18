{%- materialization aggregated_test, default -%}

  {% call statement('main', fetch_result=True) -%}
    {{ get_aggregated_test_sql(sql) }}
  {%- endcall %}

  {{ return({'relations': []}) }}

{%- endmaterialization -%}
