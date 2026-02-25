{% macro databricks__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace
    table {{ this_relation.render() }}
    shallow clone {{ defer_relation.render() }}
{% endmacro %}

{% macro create_or_replace_clone_external(this_relation, defer_relation) %}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    create or replace
    table {{ this_relation.render() }}
    shallow clone {{ defer_relation.render() }}
    {{ location_clause(catalog_relation) }}

{% endmacro %}
