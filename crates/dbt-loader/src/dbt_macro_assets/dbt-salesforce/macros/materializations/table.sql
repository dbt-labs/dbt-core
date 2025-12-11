{% materialization table, adapter='salesforce', supported_languages=['sql']%}

  {%- set identifier = model['alias'] -%}

  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table'
   ) -%}

  {# Though adapter.execute supports passing the Arrow ADBC Statement options, it is not recommended to use in any user defined macros. #}
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
