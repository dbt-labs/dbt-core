
{% macro bigquery__handle_existing_table(full_refresh, old_relation) %}
    {%- if full_refresh -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- else -%}
      {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {%- endif -%}
{% endmacro %}


{% materialization view, adapter='bigquery' -%}
    {{ return(create_or_replace_view(run_outside_transaction_hooks=False)) }}
{%- endmaterialization %}
