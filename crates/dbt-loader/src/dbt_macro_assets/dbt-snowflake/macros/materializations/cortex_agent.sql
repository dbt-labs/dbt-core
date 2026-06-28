{% materialization cortex_agent, adapter='snowflake' %}

  {% set original_query_tag = set_query_tag() %}

  {%- set comment = config.meta_get('comment') -%}
  {%- if comment is none -%}
    {%- set comment = config.get('comment', default='') -%}
  {%- endif -%}

  {%- set profile = config.meta_get('profile') -%}
  {%- if profile is none -%}
    {%- set profile = config.get('profile', default=none) -%}
  {%- endif -%}

  {%- set specification = compiled_code -%}

  {%- if specification is none or specification | trim == '' -%}
    {{ exceptions.raise_compiler_error(
        "cortex_agent materialization requires a non-empty model body "
        ~ "(the FROM SPECIFICATION YAML). Model: " ~ model['unique_id']
    ) }}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {{ get_create_cortex_agent_sql(
        relation=this,
        specification=specification,
        comment=comment,
        profile=profile
    ) }}
  {%- endcall %}

  {#-- AGENT objects have no documented standard grant model, so apply_grants
       is intentionally not wired here; grants (if any) go via post_hooks. --#}
  {{ run_hooks(post_hooks) }}

  {% do unset_query_tag(original_query_tag) %}

  {%- do return({'relations': [this]}) -%}

{% endmaterialization %}
