{%- macro build_ref_function(model) -%}
  {%- set ref_dict = {} -%}
  {#-- i mean obviously don't do this... --#}
  {%- for _ref in model.refs -%}
      {%- set resolved = ref(*_ref) -%}
      {%- do ref_dict.update({_ref | join("."): resolved | string}) -%}
  {%- endfor -%}
def ref(*args):
  refs = {{ ref_dict | tojson }}
  key = ".".join(args)
  return load_df_function(refs[key])
{%- endmacro -%}

{%- macro build_source_function(model) -%}
    {%- set source_dict = {} -%}
    {#-- i mean obviously don't do this... --#}
    {%- for _source in model.sources -%}
        {%- set resolved = source(*_source) -%}
        {%- do source_dict.update({_source | join("."): resolved | string}) -%}
    {%- endfor -%}
def source(*args):
  sources = {{ source_dict | tojson }}
  key = ".".join(args)
  return load_df_function(sources[key])
{% endmacro %}



{% macro build_config_function(model) %}

    {% set config_dict = {} %}
    {% for key, value in model.config.items() %}
        {# TODO: weird type testing with enum, would be much easier to write this logic in Python! #}
        {% if key == 'language' %}
          {% set value = 'python' %}
        {% endif %}
        {% do config_dict.update({key: value}) %}
    {% endfor %}

def config(*args, **kwargs):
    """support dbt.config().get('key') to access config values at runtime"""
    return {{ config_dict }}

{% endmacro %}

{% macro py_script_prefix(model) %}
# this part is dbt logic for get ref work, do not modify
{{ build_ref_function(model ) }}
{{ build_source_function(model ) }}
{{ build_config_function(model) }}

{{ load_df_def() }}

class this:
  """dbt.this() or dbt.this.identifier"""
  database = '{{ this.database }}'
  schema = '{{ this.schema }}'
  identifier = '{{ this.identifier }}'
  def __repr__(self):
    return '{{ this }}'


class dbt:
  config = config
  ref = ref
  source = source
  is_incremental = {{ is_incremental() }}

# COMMAND ----------
{% endmacro %}

{% macro load_df_def() %}
  {{ exceptions.raise_not_implemented(
    'load_df_def macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
