{% macro make_intermediate_relation(base_relation, suffix='__dbt_tmp') %}
  {{ return(adapter.dispatch('make_intermediate_relation', 'dbt')(base_relation, suffix)) }}
{% endmacro %}

{% macro default__make_intermediate_relation(base_relation, suffix) %}
    {{ return(default__make_temp_relation(base_relation, suffix)) }}
{% endmacro %}

{% macro make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {#-- This ensures microbatch batches get unique temp relations to avoid clobbering --#}
  {% if suffix == '__dbt_tmp' and model.batch %}
    {% set suffix = suffix ~ '_' ~ model.batch.id %}
  {% endif %}

  {{ return(adapter.dispatch('make_temp_relation', 'dbt')(base_relation, suffix)) }}
{% endmacro %}

{% macro default__make_temp_relation(base_relation, suffix) %}
    {%- set temp_identifier = base_relation.identifier ~ suffix -%}
    {%- set temp_relation = base_relation.incorporate(
                                path={"identifier": temp_identifier}) -%}

    {{ return(temp_relation) }}
{% endmacro %}

{% macro make_backup_relation(base_relation, backup_relation_type, suffix='__dbt_backup') %}
    {{ return(adapter.dispatch('make_backup_relation', 'dbt')(base_relation, backup_relation_type, suffix)) }}
{% endmacro %}

{% macro default__make_backup_relation(base_relation, backup_relation_type, suffix) %}
    {%- set backup_identifier = base_relation.identifier ~ suffix -%}
    {%- set backup_relation = base_relation.incorporate(
                                  path={"identifier": backup_identifier},
                                  type=backup_relation_type
    ) -%}
    {{ return(backup_relation) }}
{% endmacro %}


{% macro truncate_relation(relation) -%}
  {{ return(adapter.dispatch('truncate_relation', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation.render() }}
  {%- endcall %}
{% endmacro %}


{% macro get_or_create_relation(database, schema, identifier, type) -%}
  {{ return(adapter.dispatch('get_or_create_relation', 'dbt')(database, schema, identifier, type)) }}
{% endmacro %}

{% macro default__get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

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


-- a user-friendly interface into adapter.get_relation
{% macro load_cached_relation(relation) %}
  {% do return(adapter.get_relation(
    database=relation.database,
    schema=relation.schema,
    identifier=relation.identifier
  )) -%}
{% endmacro %}

-- old name for backwards compatibility
{% macro load_relation(relation) %}
    {{ return(load_cached_relation(relation)) }}
{% endmacro %}
