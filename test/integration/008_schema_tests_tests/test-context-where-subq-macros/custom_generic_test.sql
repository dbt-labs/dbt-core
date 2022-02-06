/*{# This test will fail if get_where_subquery() is missing from TestContext + TestMacroNamespace #}*/

{% test self_referential(model) %}

    {%- set relation = api.Relation.create(schema=model.schema, identifier=model.table) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- set columns_csv = columns | map(attribute='name') | list | join(', ') -%}

    select {{ columns_csv }} from {{ model }}
    limit 0

{% endtest %}
