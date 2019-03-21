
{% macro bigquery__handle_existing_table(full_refresh, non_destructive_mode, old_relation) %}
    {%- if full_refresh and not non_destructive_mode -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- else -%}
      {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {%- endif -%}
{% endmacro %}


{% materialization view, adapter='bigquery' -%}
    {{ create_or_replace_view(run_outside_transaction_hooks=False) }}
{%- endmaterialization %}
