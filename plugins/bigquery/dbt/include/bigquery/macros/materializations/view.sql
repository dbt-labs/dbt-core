
{% macro bigquery__handle_existing_table(full_refresh, old_relation) %}
    {%- if full_refresh -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- else -%}
      {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {%- endif -%}
{% endmacro %}


{% materialization view, adapter='bigquery' -%}
    {% set to_return = create_or_replace_view(run_outside_transaction_hooks=False) %}

    {% set target_relation = this.incorporate(type='view') %}
    {% do persist_docs(target_relation, model) %}

    {% do return(to_return) %}

{%- endmaterialization %}
