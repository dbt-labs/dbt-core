{% materialization table, adapter='snowflake' %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  --build model
  {% call statement('main') -%}
     {# Drop the relation if it was a view to essencially "convert" it in a table. This does lead to 
        downtime but I think it makes sense and should happen. Impact will be minimal I suspect. #}
    {% if old_relation is not none and old_relation.type == 'view' %}
      {{ log("Dropping relation " ~ old_relation ~ " because it is a view and this model is a table.") }}
      {{ drop_relation_if_exists(old_relation) }}
    {% endif %}

    {{ create_table_as(false, target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
