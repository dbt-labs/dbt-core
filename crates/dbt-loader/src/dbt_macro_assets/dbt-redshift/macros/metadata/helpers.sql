{#- Feature helpers bridging v1 adapter methods and v2 Fusion has_feature API. -#}
{% macro redshift__use_show_apis() %}
  {%- if dbt_version.startswith('2.') -%}
    {{ return(adapter.has_feature("datasharing")) }}
  {%- else -%}
    {{ return(adapter.use_show_apis()) }}
  {%- endif -%}
{% endmacro %}

{% macro redshift__use_grants_extended() %}
  {%- if dbt_version.startswith('2.') -%}
    {{ return(adapter.has_feature("grants_extended")) }}
  {%- else -%}
    {{ return(adapter.use_grants_extended()) }}
  {%- endif -%}
{% endmacro %}

{% macro redshift__drop_without_cascade() %}
  {%- if dbt_version.startswith('2.') -%}
    {{ return(adapter.has_feature("drop_without_cascade")) }}
  {%- else -%}
    {{ return(adapter.drop_without_cascade()) }}
  {%- endif -%}
{% endmacro %}
