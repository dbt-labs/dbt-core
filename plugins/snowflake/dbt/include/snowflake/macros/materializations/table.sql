{% materialization table, adapter='snowflake' %}
  {%- set identifier = model['alias'] -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  /* --TODO: Is this still up to date?
      See ../view/view.sql for more information about this relation.
  */

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set create_as_temporary = (exists_as_table and non_destructive_mode) -%}


  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}

  -- setup: if the target relation already exists, truncate or drop it (if it's a view)
  {# TODO: Would like to check this. New materialsiation makes these tests a bit moot. We should
  be able to deprecate non-destructive flag all together here. #}
  {% if non_destructive_mode -%}
    {% if exists_as_table -%}
      --noop we can do away with this step all together since the table can be replaced in Snowflake.
      {# {{ adapter.truncate_relation(old_relation) }} #}
    {% elif exists_as_view -%}
      --noop. I think we should also be able to do away with this and call a replace.
      {{ adapter.drop_relation(old_relation) }}
      {%- set old_relation = none -%}
    {%- endif %}
  {%- endif %}

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

    {{ create_or_replace_table_as(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
