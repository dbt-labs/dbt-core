{% materialization table, adapter='salesforce', supported_languages=['sql']%}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table'
   ) -%}

  {# TODO: Implement table creation #}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
