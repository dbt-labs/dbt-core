{% macro location_clause(relation) %}
  {%- if relation.catalog_type is not none -%}
    {%- if relation.location is not none -%}
    location '{{ relation.location }}{% if is_incremental() %}_tmp{% endif %}'
    {%- endif -%}
  {%- else -%}
    {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
    {%- set file_format = config.get('file_format', default='delta') -%}
    {%- set identifier = model['alias'] -%}
    {%- if location_root is not none %}
    {%- set model_path = adapter.compute_external_path(config, model, is_incremental()) %}
    location '{{ model_path }}'
    {%- elif (not relation.is_hive_metastore()) and file_format != 'delta' -%}
    {{ exceptions.raise_compiler_error(
	'Incompatible configuration: `location_root` must be set when using a non-delta file format with Unity Catalog'
    ) }}
    {%- endif %}
  {%- endif %}
{%- endmacro -%}
