{% macro date_sharded_table(base_name) %}
    {{ return(base_name ~ "[DBT__PARTITION_DATE]") }}
{% endmacro %}

{% macro grant_access_to(entity, entity_type, role, grant_target_dict) -%}
  {% do adapter.grant_access_to(entity, entity_type, role, grant_target_dict) %}
{% endmacro %}
