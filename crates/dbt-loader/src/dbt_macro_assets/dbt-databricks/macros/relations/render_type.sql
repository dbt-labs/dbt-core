{#- Replicates the DatabricksRelationType.render() method from dbt-databricks.
    Uppercases and replaces underscores with spaces to produce SQL keywords
    (e.g. "materialized_view" -> "MATERIALIZED VIEW"). -#}
{% macro render_type(relation_type) -%}
  {{- relation_type | replace("_", " ") | upper -}}
{%- endmacro %}
