{% macro run_hooks(hooks) -%}
  {% statement %}
    {% for hook in hooks %}
      {{ hook }};
    {% endfor %}
  {% endstatement %}
{% endmacro %}

{% materialization view -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% if non_destructive_mode and existing_type == 'view' -%}
    -- noop
  {%- else -%}
    {% statement %}
      create view "{{ schema }}"."{{ identifier }}__dbt_tmp" as (
        {{ sql }}
      );
    {% endstatement %}
  {%- endif %}

  {{ run_hooks(post_hooks) }}

  -- cleanup
  {% if non_destructive_mode and existing_type == 'view' -%}
    -- noop
  {%- else -%}
    {% if existing_type is not none -%}
      {{ adapter.drop(identifier, existing_type) }}
    {%- endif %}

    {{ adapter.rename(tmp_identifier, identifier) }}
  {%- endif %}
{%- endmaterialization %}
