{#
    Dispatch strategies by name, optionally qualified to a package
#}
{% macro strategy_dispatch(name) -%}
{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        Could not find package '{{package_name}}', called with '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}

  {%- set search_name = 'snapshot_' ~ name ~ '_strategy' -%}

  {% if search_name not in package_context %}
    {% set error_msg %}
        The specified strategy macro '{{name}}' was not found in package '{{ package_name }}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  {{ return(package_context[search_name]) }}
{%- endmacro %}


{#
    Create SCD Hash SQL fields cross-db
#}
{% macro snapshot_hash_arguments(args) -%}
  {{ adapter_macro('snapshot_hash_arguments', args) }}
{%- endmacro %}


{% macro default__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{#
    Get the current time cross-db
#}
{% macro snapshot_get_time() -%}
  {{ adapter_macro('snapshot_get_time') }}
{%- endmacro %}

{% macro default__snapshot_get_time() -%}
  {{ current_timestamp() }}
{%- endmacro %}


{#
    Core strategy definitions
#}
{% macro snapshot_timestamp_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = config['updated_at'] %}

    {% set row_changed_expr -%}
        ({{ snapshotted_rel }}.{{ updated_at }} < {{ current_rel }}.{{ updated_at }})
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}


{% macro snapshot_check_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = snapshot_get_time() %}

    {% if check_cols_config == 'all' %}
        {% set check_cols = get_columns_in_query(node['injected_sql']) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {% set row_changed_expr -%}
        (
        {% for col in check_cols %}
            {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
            or
            ({{ snapshotted_rel }}.{{ col }} is null) != ({{ current_rel }}.{{ col }} is null)
            {%- if not loop.last %} or {% endif %}

        {% endfor %}
        )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}
