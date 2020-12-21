{% materialization copy, adapter='bigquery' -%}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}

  {# there should be exactly one ref or exactly one source #}
  {% set destination = this.incorporate(type='table') %}

  {% set dependency_type = none %}
  {% if (model.refs | length) == 1 and (model.sources | length) == 0 %}
    {% set dependency_type = 'ref' %}
  {% elif (model.refs | length) == 0 and (model.sources | length) == 1 %}
    {% set dependency_type = 'source' %}
  {% else %}
    {% set msg %}
        Expected exactly one ref or exactly one source, instead got {{ model.refs | length }} models and {{ model.sources | length }} sources.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}

  {% if dependency_type == 'ref' %}
    {% set src =  ref(*model.refs[0]) %}
  {% else %}
    {% set src =  source(*model.sources[0]) %}
  {% endif %}

  {%- set result_str = adapter.copy_table(
      src,
      destination,
      config.get('copy_materialization', 'table')) -%}

  {{ store_result('main', response=result_str) }}

  {# Clean up #}
  {{ run_hooks(post_hooks) }}
  {{ adapter.commit() }}

  {{ return({'relations': [destination]}) }}
{%- endmaterialization %}
