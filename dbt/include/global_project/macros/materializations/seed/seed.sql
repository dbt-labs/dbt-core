
{% macro create_csv_table(model) -%}
  {{ adapter_macro('create_csv_table', model) }}
{%- endmacro %}

{% macro reset_csv_table(model, full_refresh, existing) -%}
  {{ adapter_macro('reset_csv_table', model, full_refresh, existing) }}
{%- endmacro %}

{% macro load_csv_rows(model) -%}
  {{ adapter_macro('load_csv_rows', model) }}
{%- endmacro %}

{% macro default__create_csv_table(model) %}
  {%- set agate_table = model['agate_table'] -%}
  {%- set column_override = model['config'].get('column_types', {}) -%}

  {% set sql %}
    create table {{ adapter.quote(model['schema']) }}.{{ adapter.quote(model['name']) }} (
        {% for col_name in agate_table.column_names %}
            {% set inferred_type = adapter.convert_type(agate_table, loop.index0) %}
            {% set type = column_override.get(col_name, inferred_type) %}
            {{ col_name | string }} {{ type }} {% if not loop.last %}, {% endif %}
        {% endfor %}
    )
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{% macro default__reset_csv_table(model, full_refresh, existing) %}
    {% set sql = "" %}
    {% if full_refresh %}
        {{ drop_if_exists(existing, model['schema'], model['name']) }}
        {% set sql = create_csv_table(model) %}
    {% else %}
        {{ adapter.truncate(model['schema'], model['name']) }}
        {% set sql = "truncate table " ~ adapter.quote(model['schema']) ~ "." ~ adapter.quote(model['name']) %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}


{% macro default__load_csv_rows(model) %}
    {% set agate_table = model['agate_table'] %}
    {% set cols_sql = ", ".join(agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(10000) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% set _ = bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ adapter.quote(model['schema']) }}.{{ adapter.quote(model['name']) }} ({{ cols_sql }}) values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    %s
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% set _ = adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% set _ = statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}


{% materialization seed, default %}

  {%- set identifier = model['name'] -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}
  {%- set csv_table = model["agate_table"] -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = "" %}
  {% if existing_type and existing_type != 'table' %}
    {{ dbt.exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(identifier)) }}
  {% elif existing_type is none%}
    {% set create_table_sql = create_csv_table(model) %}
  {% else %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, existing) %}
  {% endif %}

  {% set status = 'CREATE' if full_refresh_mode else 'INSERT' %}
  {% set num_rows = (csv_table.rows | length) %}
  {% set sql = load_csv_rows(model) %}

  {% call noop_statement('main', status ~ ' ' ~ num_rows) %}
    {{ create_table_sql }};
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
