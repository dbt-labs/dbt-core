{% macro rename_relation(from_relation, to_relation) -%}
  {{ return(adapter.dispatch('rename_relation', 'dbt')(from_relation, to_relation)) }}
{% endmacro %}

{% macro default__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}
