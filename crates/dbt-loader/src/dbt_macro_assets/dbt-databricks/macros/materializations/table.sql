{% materialization table, adapter = 'databricks', supported_languages=['sql', 'python'] %}
  {{ log("MATERIALIZING TABLE") }}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set tblproperties = config.get('tblproperties') -%}
  {%- set tags = config.get('databricks_tags') -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is delta, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {% if old_relation and (not (old_relation.is_delta and config.get('file_format', default='delta') == 'delta')) or (old_relation.is_materialized_view or old_relation.is_streaming_table) -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  -- build model

  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% if language=="python" %}
    {% do apply_tblproperties(target_relation, tblproperties) %}
  {% endif %}
  {%- do apply_tags(target_relation, tags) -%}

  {% do persist_docs(target_relation, model, for_relation=language=='python') %}

  {% do persist_constraints(target_relation, model) %}

  {% do optimize(target_relation) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
