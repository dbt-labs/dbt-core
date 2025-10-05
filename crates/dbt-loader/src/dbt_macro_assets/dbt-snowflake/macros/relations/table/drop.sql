{% macro snowflake__drop_table(relation) %}
    {#-- CASCADE is not supported in catalog-linked databases --#}
    {#-- core stashes this information on the relation; we can't so we do some catalogs.yml hacks instead #}
    {#-- TODO redesign the catalog_relation type to sidestep this whole problem #}
    {%- set catalog_relation = adapter.build_catalog_relation(relation.database) -%}
    {% if snowflake__is_catalog_linked_database(relation=relation) or snowflake__is_catalog_linked_database(catalog_relation=catalog_relation) %}
        drop table if exists {{ relation }}
    {% else %}
        drop table if exists {{ relation }} cascade
    {% endif %}
{% endmacro %}
