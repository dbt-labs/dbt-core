{% macro liquid_clustered_cols() -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- set auto_cluster = config.get('auto_liquid_cluster', validator=validation.any[boolean]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    CLUSTER BY ({{ cols | join(', ') }})
    {%- elif auto_cluster -%}
    CLUSTER BY AUTO
  {%- endif %}
{%- endmacro -%}

{% macro apply_liquid_clustered_cols(target_relation) -%}
  {%- set cols = config.get('liquid_clustered_by', validator=validation.any[list, basestring]) -%}
  {%- set auto_cluster = config.get('auto_liquid_cluster', validator=validation.any[boolean]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {%- call statement('set_cluster_by_columns') -%}
        ALTER {{ target_relation.type }} {{ target_relation.render() }} CLUSTER BY ({{ cols | join(', ') }})
    {%- endcall -%}
  {%- elif auto_cluster -%}
    {%- call statement('set_cluster_by_auto') -%}
        ALTER {{ target_relation.type }} {{ target_relation.render() }} CLUSTER BY AUTO
    {%- endcall -%}
  {%- endif %}
{%- endmacro -%}