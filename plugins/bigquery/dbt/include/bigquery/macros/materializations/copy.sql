{% materialization copy, adapter='bigquery' -%}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}

  {% set destination = this.incorporate(type='table') %}


  {# Cycle over ref() and source() to create source tables array #}
  {% set src = [] %}
  {% for ref_table in model.refs %}
    {{ src.append(ref(*ref_table)) }}
  {% endfor %}

  {% for src_table in model.sources %}
    {{ src.append(source(*src_table)) }}
  {% endfor %}

  {# Call adapter's copy_table function #}
  {%- set result_str = adapter.copy_table(
      src,
      destination,
      config.get('copy_materialization', default = 'table')) -%}

  {{ store_result('main', response=result_str) }}

  {# Clean up #}
  {{ run_hooks(post_hooks) }}
  {{ adapter.commit() }}

  {{ return({'relations': [destination]}) }}
{%- endmaterialization %}
