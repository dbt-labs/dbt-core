{% materialization view -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {% for hook in pre_hooks %}
    {% statement %}
      {{ hook }};
    {% endstatement %}
  {% endfor %}

  -- build model
  {% if non_destructive_mode and existing_type == 'view' -%}
    -- noop
  {%- else -%}
    {% statement capture_result %}
      create view "{{ schema }}"."{{ identifier }}__dbt_tmp" as (
        {{ sql }}
      );
    {% endstatement %}
  {%- endif %}

  {% for hook in post_hooks %}
    {% statement %}
      {{ hook }};
    {% endstatement %}
  {% endfor %}

  -- cleanup
  {% if non_destructive_mode and existing_type == 'view' -%}
    -- noop
  {%- else -%}
    {% if existing_type is not none -%}
      {{ adapter.drop(identifier, existing_type) }}
    {%- endif %}

    {{ adapter.rename(tmp_identifier, identifier) }}
  {%- endif %}

  {{ adapter.commit() }}

{%- endmaterialization %}
