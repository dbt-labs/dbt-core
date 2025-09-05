{% materialization table, adapter='salesforce', supported_languages=['sql']%}

  {%- set identifier = model['alias'] -%}

  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table'
   ) -%}

  {# The options here are unstable and susceptible to breaking changes #}
  {% do adapter.execute(
    sql=compiled_code,
    auto_begin=False,
    fetch=False,
    limit=None,
    options={
      "adbc.salesforce.dc.dlo.primary_key": config.get('primary_key', default=None),
      "adbc.salesforce.dc.dlo.category": config.get('category', default='Profile'),
      "adbc.salesforce.dc.dlo.target_dlo": identifier
    })%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
