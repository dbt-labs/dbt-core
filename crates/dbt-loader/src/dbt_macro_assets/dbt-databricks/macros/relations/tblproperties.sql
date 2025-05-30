{% macro tblproperties_clause() -%}
  {{ return(adapter.dispatch('tblproperties_clause', 'dbt')()) }}
{%- endmacro -%}

{% macro databricks__tblproperties_clause(tblproperties=None) -%}
  {%- set tblproperties = adapter.update_tblproperties_for_iceberg(config, tblproperties) -%}
  {%- if tblproperties != {} %}
    tblproperties (
      {%- for prop in tblproperties -%}
      '{{ prop }}' = '{{ tblproperties[prop] }}' {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}

{% macro apply_tblproperties(relation, tblproperties) -%}
  {% set tblproperty_statment = databricks__tblproperties_clause(tblproperties) %}
  {% if tblproperty_statment %}
    {%- call statement('apply_tblproperties') -%}
      ALTER {{ relation.type }} {{ relation.render() }} SET {{ tblproperty_statment}}
    {%- endcall -%}
  {% endif %}
{%- endmacro -%}
