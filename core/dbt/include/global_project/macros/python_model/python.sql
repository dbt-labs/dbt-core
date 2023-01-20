{% macro build_ref_function(model) %}

    {%- set ref_dict = {} -%}
    {%- for _ref in model.refs -%}
        {%- set resolved = ref(*_ref) -%}
        {%- do ref_dict.update({_ref | join("."): resolved | string | replace('"', '\"')}) -%}
    {%- endfor -%}

def ref(*args,dbt_load_df_function):
    refs = {{ ref_dict | tojson }}
    key = ".".join(args)
    return dbt_load_df_function(refs[key])

{% endmacro %}

{% macro build_source_function(model) %}

    {%- set source_dict = {} -%}
    {%- for _source in model.sources -%}
        {%- set resolved = source(*_source) -%}
        {%- do source_dict.update({_source | join("."): resolved | string | replace('"', '\"')}) -%}
    {%- endfor -%}

def source(*args, dbt_load_df_function):
    sources = {{ source_dict | tojson }}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])

{% endmacro %}

{% macro build_config_dict(model) %}
    {%- set config_dict = {} -%}
    {% set config_dbt_used = zip(model.config.config_keys_used, model.config.config_keys_defaults) | list %}
    {%- for key, default in config_dbt_used -%}
        {# weird type testing with enum, would be much easier to write this logic in Python! #}
        {%- if key == "language" -%}
          {%- set value = "python" -%}
        {%- endif -%}
        {%- set value = model.config.get(key, default) -%}
        {%- do config_dict.update({key: value}) -%}
    {%- endfor -%}
config_dict = {{ config_dict }}
{% endmacro %}

{% macro py_script_postfix(model) %}
# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

{{ build_ref_function(model ) }}
{{ build_source_function(model ) }}
{{ build_config_dict(model) }}

class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "{{ this.database }}"
    schema = "{{ this.schema }}"
    identifier = "{{ this.identifier }}"
    {% set this_relation_name = this | string | replace('"', '\\"') %}
    def __repr__(self):
        return "{{ this_relation_name  }}"


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = {{ is_incremental() }}

# COMMAND ----------
{{py_script_comment()}}
{% endmacro %}

{#-- entry point for add instuctions for running compiled_code --#}
{%macro py_script_comment()%}
{%endmacro%}
