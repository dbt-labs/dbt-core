{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- if existing_type is not none -%}
    {%- if existing_type == 'table' and not flags.FULL_REFRESH -%}
      {# this is only intended for date partitioned tables, but we cant see that field in the context #}
      {% set error_message -%}
        Trying to create model '{{ identifier }}' as a view, but it already exists as a table.
        Either drop the '{{ schema }}.{{ identifier }}' table manually, or use --full-refresh
      {%- endset %}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% set result = adapter.execute_model(model, 'view') %}
  {{ store_result('main', status=result) }}

{%- endmaterialization %}


{% macro make_date_partitioned_table(model, dates, should_create, verbose=False) %}

  {% if should_create %}
      {{ adapter.make_date_partitioned_table(model.schema, model.name) }}
  {% endif %}

  {% for date in dates %}
    {% set date = (date | string) %}
    {% if verbose %}
        {% set table_start_time = modules.datetime.datetime.now().strftime("%H:%M:%S") %}
        {{ log(table_start_time ~ ' | -> Running for day ' ~ date, info=True) }}
    {% endif %}

    {% set fixed_sql = model['injected_sql'] | replace('[DBT__PARTITION_DATE]', date) %}
    {% set _ = adapter.execute_model(model, 'table', fixed_sql, decorator=date) %}
  {% endfor %}

  {% set num_days = dates | length %}
  {% if num_days == 1 %}
      {% set result_str = 'CREATED 1 PARTITION' %}
  {% else %}
      {% set result_str = 'CREATED ' ~ num_days ~ ' PARTITIONS' %}
  {% endif %}

  {{ return(result_str) }}

{% endmacro %}

{% materialization table, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}
  {%- set verbose = config.get('verbose', False) -%}
  {%- set partitions = config.get('partitions') -%}

  {% if partitions %}
      {% if partitions is number or partitions is string %}
        {% set partitions = [(partitions | string)] %}
      {% endif %}

      {% if partitions is not iterable %}
        {{ exceptions.raise_compiler_error("Provided `partitions` configuration is not a list. Got: " ~ partitions, model) }}
      {% endif %}
  {% endif %}

  {#
      Since dbt uses WRITE_TRUNCATE mode for tables, we only need to drop this thing
      if it is not a table. If it _is_ already a table, then we can overwrite it without downtime
  #}
  {%- if existing_type is not none and existing_type != 'table' -%}
      {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% if partitions %}
      {% set result = make_date_partitioned_table(model, partitions, (existing_type != 'table'), verbose) %}
  {% else %}
      {% set result = adapter.execute_model(model, 'table') %}
  {% endif %}

  {{ store_result('main', status=result) }}

{% endmaterialization %}

{% materialization incremental, adapter='bigquery' -%}

  {{ exceptions.materialization_not_available(model, 'bigquery') }}

{% endmaterialization %}
