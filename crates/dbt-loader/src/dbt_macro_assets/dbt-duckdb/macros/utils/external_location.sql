{%- macro external_location(relation, config) -%}
  {%- if config.get('options', {}).get('partition_by') is none -%}
    {%- set format = config.get('format', 'parquet') -%}
    {{- adapter.external_root() }}/{{ relation.identifier }}.{{ format }}
  {%- else -%}
    {{- adapter.external_root() }}/{{ relation.identifier }}
  {%- endif -%}
{%- endmacro -%}
