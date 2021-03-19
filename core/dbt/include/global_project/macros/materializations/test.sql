{%- materialization test, default -%}

  {% call statement('main', fetch_result=True) -%}
    select count(*) as validation_errors
    from (
      {{ sql }}
    ) _dbt_internal_test
  {%- endcall %}

{%- endmaterialization -%}
