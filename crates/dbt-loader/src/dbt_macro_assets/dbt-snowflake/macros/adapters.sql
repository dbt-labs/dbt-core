-- funcsign: (string, dict[string, model]) -> string
{% macro get_column_comment_sql(column_name, column_dict) -%}
  {% if (column_name|upper in column_dict) -%}
    {% set matched_column = column_name|upper -%}
  {% elif (column_name|lower in column_dict) -%}
    {% set matched_column = column_name|lower -%}
  {% elif (column_name in column_dict) -%}
    {% set matched_column = column_name -%}
  {% else -%}
    {% set matched_column = None -%}
  {% endif -%}
  {% if matched_column -%}
    {{ adapter.quote(column_name) }} COMMENT $${{ column_dict[matched_column]['description'] | replace('$', '[$]') }}$$
  {%- else -%}
    {{ adapter.quote(column_name) }} COMMENT $$$$
  {%- endif -%}
{% endmacro %}

-- funcsign: (dict[string, model], list[string]) -> string
{% macro get_persist_docs_column_list(model_columns, query_columns) %}
(
  {% for column_name in query_columns %}
    {{ get_column_comment_sql(column_name, model_columns) }}
    {{- ", " if not loop.last else "" }}
  {% endfor %}
)
{% endmacro %}

-- funcsign: (relation) -> list[api.column]
{% macro snowflake__get_columns_in_relation(relation) -%}
  {%- set sql -%}
    describe table {{ relation.render() }}
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation {{ relation.render() }}! dbt can only get
      information about relations with fewer than {{ maximum }} columns.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}

  {% set columns = [] %}
  {% for row in result %}
    {% do columns.append(api.Column.from_description(row['name'], row['type'])) %}
  {% endfor %}
  {% do return(columns) %}
{% endmacro %}

-- funcsign: (relation) -> agate_table
{% macro snowflake__show_object_metadata(relation) %}
  {%- set sql -%}
    show objects in {{ relation.include(identifier=False) }} starts with '{{ relation.identifier }}' limit 1
  {%- endset -%}

  {%- set result = run_query(sql) -%}
  {{ return(result) }}
{% endmacro %}

-- funcsign: (string) -> agate_table
{% macro snowflake__list_schemas(database) -%}
  {# 10k limit from here: https://docs.snowflake.net/manuals/sql-reference/sql/show-schemas.html#usage-notes #}
  {% set maximum = 10000 %}
  {% set sql -%}
    show terse schemas in database {{ database }}
    limit {{ maximum }}
  {%- endset %}
  {% set result = run_query(sql) %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many schemas in database {{ database }}! dbt can only get
      information about databases with fewer than {{ maximum }} schemas.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {{ return(result) }}
{% endmacro %}


{% macro snowflake__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
        select count(*)
        from {{ information_schema }}.schemata
        where upper(schema_name) = upper('{{ schema }}')
            and upper(catalog_name) = upper('{{ information_schema.database }}')
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}

-- funcsign: (relation, string, string) -> string
{% macro snowflake__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter {{ relation.get_ddl_prefix_for_alter() }} table {{ relation.render() }} alter {{ adapter.quote(column_name) }} set data type {{ new_column_type }};
  {% endcall %}
{% endmacro %}

-- funcsign: (relation, string) -> string
{% macro snowflake__alter_relation_comment(relation, relation_comment) -%}
    {%- if relation.is_dynamic_table -%}
        {%- set relation_type = 'dynamic table' -%}
    {%- else -%}
        {%- set relation_type = relation.type -%}
    {%- endif -%}

    {%- if relation.is_iceberg_format() -%}
        alter iceberg table {{ relation.render() }} set comment = $${{ relation_comment | replace('$', '[$]') }}$$;
    {%- else -%}
        comment on {{ relation_type }} {{ relation.render() }} IS $${{ relation_comment | replace('$', '[$]') }}$$;
    {%- endif -%}
{% endmacro %}

-- funcsign: (relation, dict[string, model]) -> string
{% macro snowflake__alter_column_comment(relation, column_dict) -%}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {% if relation.is_dynamic_table -%}
        {% set relation_type = "table" %}
    {% else -%}
        {% set relation_type = relation.type %}
    {% endif %}
    alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} alter
    {% for column_name in existing_columns if (column_name in existing_columns) or (column_name|lower in existing_columns) %}
        {{ get_column_comment_sql(column_name, column_dict) }} {{- ',' if not loop.last else ';' }}
    {% endfor %}
{% endmacro %}

-- funcsign: () -> agate_table
{% macro get_current_query_tag() -%}
  {{ return(run_query("show parameters like 'query_tag' in session").rows[0]['value']) }}
{% endmacro %}

-- funcsign: () -> optional[agate_table]
{% macro set_query_tag() -%}
    {{ return(adapter.dispatch('set_query_tag', 'dbt')()) }}
{% endmacro %}

-- funcsign: () -> optional[agate_table]
{% macro snowflake__set_query_tag() -%}
  {% set new_query_tag = config.get('query_tag') %}
  {% if new_query_tag %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}

-- funcsign: (optional[string]) -> string
{% macro unset_query_tag(original_query_tag) -%}
    {{ return(adapter.dispatch('unset_query_tag', 'dbt')(original_query_tag)) }}
{% endmacro %}

-- funcsign: (optional[string]) -> string
{% macro snowflake__unset_query_tag(original_query_tag) -%}
  {% set new_query_tag = config.get('query_tag') %}
  {% if new_query_tag %}
    {% if original_query_tag %}
      {{ log("Resetting query_tag to '" ~ original_query_tag ~ "'.") }}
      {% do run_query("alter session set query_tag = '{}'".format(original_query_tag)) %}
    {% else %}
      {{ log("No original query_tag, unsetting parameter.") }}
      {% do run_query("alter session unset query_tag") %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro snowflake__get_column_data_type_for_alter(relation, column) %}
  {#
    Helper macro to get the correct data type for ALTER TABLE operations.
    For Iceberg tables, we need to handle VARCHAR constraints differently because
    Snowflake Iceberg tables only support max length (134,217,728) or STRING directly.

    This fixes the bug where dbt generates VARCHAR(16777216) for new columns which
    is not supported by Snowflake Iceberg tables.
  #}
  {% if relation.is_iceberg_format() and column.is_string() %}
    {% set data_type = column.data_type.upper() %}
    {% if data_type.startswith('CHARACTER VARYING') or data_type.startswith('VARCHAR') %}
      {#
        For Iceberg tables, convert any VARCHAR specification to STRING.
        This handles cases where:
        - dbt auto-generates VARCHAR(16777216) for columns without explicit size
        - users specify VARCHAR with any size (even the max 134217728)
        Using STRING is more compatible and avoids size-related errors.
      #}
      STRING
    {% else %}
      {# Keep other string types like TEXT as-is #}
      {{ column.data_type }}
    {% endif %}
  {% else %}
    {{ column.data_type }}
  {% endif %}
{% endmacro %}

-- funcsign: (relation, optional[list[base_column]], optional[list[base_column]]) -> string
{% macro snowflake__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if relation.is_dynamic_table -%}
        {% set relation_type = "dynamic table" %}
    {% else -%}
        {% set relation_type = relation.type %}
    {% endif %}

    {% if add_columns %}

    {% set sql -%}
       alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} add column
          {% for column in add_columns %}
            {{ adapter.quote(column.name) }} {{ snowflake__get_column_data_type_for_alter(relation, column) }}{{ ',' if not loop.last }}
          {% endfor %}
    {%- endset -%}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} drop column
            {% for column in remove_columns %}
                {{ adapter.quote(column.name) }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}

{% macro snowflake__is_catalog_linked_database(relation=none, catalog_relation=none) -%}
    {#-- Helper macro to detect if we're in a catalog-linked database context --#}
    {#-- CORE DISCREPANCY: there's supposed to be a relation.catalog based arm here. This is a core
      -- hack that won't work in Fusion. #}
    {%- if catalog_relation is not none -%}
        {#-- Direct catalog_relation object provided --#}
        {%- if catalog_relation|attr('catalog_linked_database') -%}
            {{ return(true) }}
        {%- else -%}
            {{ return(false) }}
        {%- endif -%}
    {%- elif relation and relation.config -%}
        {%- set catalog_relation = adapter.build_catalog_relation(relation) -%}
        {%- if catalog_relation is not none and catalog_relation|attr('catalog_linked_database') -%}
            {{ return(true) }}
        {%- else -%}
            {{ return(false) }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}

-- funcsign: (string) -> string
{% macro snowflake_dml_explicit_transaction(dml) %}
  {#
    Use this macro to wrap all INSERT, MERGE, UPDATE, DELETE, and TRUNCATE
    statements before passing them into run_query(), or calling in the 'main' statement
    of a materialization
  #}
  {% set dml_transaction -%}
    begin;
    {{ dml }};
    commit;
  {%- endset %}

  {% do return(dml_transaction) %}

{% endmacro %}

-- funcsign: (relation) -> string
{% macro snowflake__truncate_relation(relation) -%}
  {% set truncate_dml %}
    truncate table {{ relation.render() }}
  {% endset %}
  {% call statement('truncate_relation') -%}
    {% if snowflake__is_catalog_linked_database(relation=config.model) %}
        {{ truncate_dml }}
    {% else %}
      {{ snowflake_dml_explicit_transaction(truncate_dml) }}
    {% endif %}
  {%- endcall %}
{% endmacro %}
