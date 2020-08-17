{% materialization copy, adapter='bigquery' -%}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}

  {# execute the macro sql #}
  {{ write(sql) }}
  {{ store_result(name='main', status='COPY TABLE') }}

  {# Clean up #}
  {{ run_hooks(post_hooks) }}
  {{ adapter.commit() }}

  { return({'relations': [context['destination']]}) }
{%- endmaterialization %}
