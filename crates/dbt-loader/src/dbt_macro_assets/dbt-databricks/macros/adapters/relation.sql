{% macro databricks__make_temp_relation(base_relation, suffix='__dbt_tmp', as_table=False) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {%- if as_table -%}
        {% set tmp_relation = api.Relation.create(
            identifier=tmp_identifier,
            schema=base_relation.schema,
            database=base_relation.database,
            type='table') %}
    {%- else -%}
        -- This is intentional - it's to create a view relation instead of a temp view
        -- since DBX v2 api doesn't support session
        {% set tmp_relation = api.Relation.create(
            identifier=tmp_identifier,
            schema=base_relation.schema,
            database=base_relation.database,
            type='view') %}
    {%- endif -%}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro databricks__get_or_create_relation(database, schema, identifier, type, needs_information=False) %}
  {%- set target_relation = adapter.get_relation(
            database=database,
            schema=schema,
            identifier=identifier,
            needs_information=needs_information) %}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}
