
{% macro create_csv_table(model, agate_table) -%}
  {{ adapter.dispatch('create_csv_table', 'dbt')(model, agate_table) }}
{%- endmacro %}

{% macro default__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}

{% macro reset_csv_table(model, full_refresh, old_relation, agate_table) -%}
  {{ adapter.dispatch('reset_csv_table', 'dbt')(model, full_refresh, old_relation, agate_table) }}
{%- endmacro %}

{% macro default__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model, agate_table) %}
    {% else %}
        {{ adapter.truncate_relation(old_relation) }}
        {% set sql = "truncate table " ~ old_relation %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}

{% macro get_binding_char() -%}
  {{ adapter.dispatch('get_binding_char', 'dbt')() }}
{%- endmacro %}

{% macro default__get_binding_char() %}
  {{ return('%s') }}
{% endmacro %}

{% macro get_batch_size() -%}
  {{ return(adapter.dispatch('get_batch_size', 'dbt')()) }}
{%- endmacro %}

{% macro default__get_batch_size() %}
  {{ return(10000) }}
{% endmacro %}

{% macro get_seed_column_quoted_csv(model, column_names) %}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(adapter.quote_seed_column(col, quote_seed_column)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}

{% macro load_csv_rows(model, agate_table) -%}
  {{ adapter.dispatch('load_csv_rows', 'dbt')(model, agate_table) }}
{%- endmacro %}

{% macro default__load_csv_rows(model, agate_table) %}

  {% set batch_size = get_batch_size() %}

  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set bindings = [] %}

  {% set statements = [] %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set bindings = [] %}

      {% for row in chunk %}
          {% do bindings.extend(row) %}
      {% endfor %}

      {% set sql %}
          insert into {{ this.render() }} ({{ cols_sql }}) values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {{ get_binding_char() }}
                  {%- if not loop.last%},{%- endif %}
              {%- endfor -%})
              {%- if not loop.last%},{%- endif %}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {# Return SQL so we can render it out into the compiled files #}
  {{ return(statements[0]) }}
{% endmacro %}

{% materialization seed, default %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation)) }}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}

  {% set code = 'CREATE' if full_refresh_mode else 'INSERT' %}
  {% set rows_affected = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    {{ create_table_sql }};
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {% set target_relation = this.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {% if full_refresh_mode or not exists_as_table %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
