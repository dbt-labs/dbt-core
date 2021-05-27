{%- materialization test, default -%}

  {% set relations = [] %}

  {% if should_store_failures() %}

    {% set identifier = model['alias'] %}
    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
    {% set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type='table') -%} %}
    
    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}
    
    {% call statement(auto_begin=True) %}
        {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}
    
    {% do relations.append(target_relation) %}
  
    {% set main_sql %}
        select count(*) as validation_errors
        from {{ target_relation }}
    {% endset %}
    
    {{ adapter.commit() }}
  
  {% else %}

      {% set main_sql %}
          select count(*) as validation_errors
          from (
            {{ sql }}
          ) _dbt_internal_test
      {% endset %}
  
  {% endif %}

  {% call statement('main', fetch_result=True) -%}
    {{ main_sql }}
  {%- endcall %}
  
  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
