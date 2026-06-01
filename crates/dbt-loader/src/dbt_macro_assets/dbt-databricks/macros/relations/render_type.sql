{#- Replicates the DatabricksRelationType.render() method from dbt-databricks.
    Uppercases and replaces underscores with spaces to produce SQL keywords
    (e.g. "materialized_view" -> "MATERIALIZED VIEW").

    DIVERGENCE: Upstream v1 dbt-databricks does not define this macro; instead each
    call site uses `relation.type.render()` directly because v1 represents
    `relation.type` as a `DatabricksRelationType` Python enum with a `.render()` method.
    In Fusion (dbt 2.x), `relation.type` is a plain string and there is no `.render()`,
    so we provide this macro and call it instead. To stay compatible with both
    runtimes from the same macro body, dispatch on `dbt_version` and call the
    underlying `.render()` method when running under dbt-core (1.x). -#}
{% macro render_type(relation_type) -%}
  {% if dbt_version.startswith('2.') %}
    {{- relation_type | replace("_", " ") | upper -}}
  {% else %}
    {{- relation_type.render() -}}
  {% endif %}
{%- endmacro %}
