
{% macro dist(dist) %}
  {%- if dist is not none -%}
      {%- set dist = dist.strip().lower() -%}

      {%- if dist in ['all', 'even'] -%}
        diststyle {{ dist }}
      {%- elif dist == "auto" -%}
      {%- else -%}
        diststyle key distkey ({{ dist }})
      {%- endif -%}

  {%- endif -%}
{%- endmacro -%}


{% macro sort(sort_type, sort) %}
  {%- if sort is not none %}
      {{ sort_type | default('compound', boolean=true) }} sortkey(
      {%- if sort is string -%}
        {%- set sort = [sort] -%}
      {%- endif -%}
      {%- for item in sort -%}
        {{ item }}
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
      )
  {%- endif %}
{%- endmacro -%}


{% macro redshift__create_table_as(temporary, relation, sql) -%}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set backup = config.get('backup') -%}

  {{ sql_header if sql_header is not none }}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {{ get_table_columns_and_constraints() }}
    {{ get_assert_columns_equivalent(sql) }}
    {%- set sql = get_select_subquery(sql) %}
    {% if backup == false -%}backup no{%- endif %}
    {{ dist(_dist) }}
    {{ sort(_sort_type, _sort) }}
  ;

  insert into {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    (
      {{ sql }}
    )
  ;

  {%- else %}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if backup == false -%}backup no{%- endif %}
    {{ dist(_dist) }}
    {{ sort(_sort_type, _sort) }}
  as (
    {{ sql }}
  );

  {%- endif %}
{%- endmacro %}


{% macro redshift__create_view_as(relation, sql) -%}
  {%- set binding = config.get('bind', default=True) -%}

  {% set bind_qualifier = '' if binding else 'with no schema binding' %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %} as (
    {{ sql }}
  ) {{ bind_qualifier }};
{% endmacro %}


{# DIVERGENCE BEGIN: Fusion uses adapter.has_feature("datasharing") to choose between
   SQL DDL and the postgres__ fallback. `has_feature` is a Fusion-only API; under
   dbt-core (1.x) it is unavailable. v1 dbt-redshift unconditionally delegates to the
   postgres__ macro, so do that in the else branch. #}
{% macro redshift__create_schema(relation) -%}
  {%- if not dbt_version.startswith('2.') -%}{{ return(postgres__create_schema(relation)) }}{%- endif -%}
  {%- if adapter.has_feature("datasharing") -%}
    {%- call statement('create_schema') -%}
      create schema if not exists {{ relation.without_identifier() }}
    {%- endcall -%}
  {%- else -%}
    {{ postgres__create_schema(relation) }}
  {%- endif -%}
{% endmacro %}


{% macro redshift__drop_schema(relation) -%}
  {%- if not dbt_version.startswith('2.') -%}{{ return(postgres__drop_schema(relation)) }}{%- endif -%}
  {%- if adapter.has_feature("datasharing") -%}
    {%- call statement('drop_schema') -%}
      drop schema if exists {{ relation.without_identifier() }} cascade
    {%- endcall -%}
  {%- else -%}
    {{ postgres__drop_schema(relation) }}
  {%- endif -%}
{% endmacro %}
{# DIVERGENCE END #}


{% macro redshift__get_columns_in_relation(relation) -%}
  {# relation from temp tables does not have a database or schema. #}
  {# use legacy pattern until SHOW COLUMNS supports temp tables #}

  {#- DIVERGENCE BEGIN: upstream v1 dbt-redshift unconditionally runs the legacy
      information_schema query; Fusion gates on adapter.has_feature("datasharing").
      `has_feature` is a Fusion-only API, so under dbt-core (1.x) short-circuit to
      the legacy path; the Fusion branch is preserved unchanged. -#}
  {%- if not dbt_version.startswith('2.') -%}{{ return(redshift__get_columns_in_relation_legacy(relation)) }}{%- endif -%}
  {% if adapter.has_feature("datasharing") and relation.database and relation.schema %}
    {{ return(redshift__get_columns_in_relation_show(relation)) }}
  {% else %}
    {{ return(redshift__get_columns_in_relation_legacy(relation)) }}
  {% endif %}
  {#- DIVERGENCE END -#}
{% endmacro %}


{% macro redshift__get_columns_in_relation_show(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    SHOW COLUMNS FROM TABLE {{ adapter.quote(relation.database) }}.{{ adapter.quote(relation.schema) }}.{{ adapter.quote(relation.identifier) }}
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {% set columns = [] %}
  {% for row in table %}
    {# filter out Redshift internal pg.dropped markers that can appear after ALTER TABLE DROP COLUMN #}
    {% if 'pg.dropped.' not in row['column_name'] %}
      {% do columns.append(api.Column(
        column=row['column_name'],
        dtype=row['data_type'],
        char_size=row['character_maximum_length'],
        numeric_precision=row['numeric_precision'],
        numeric_scale=row['numeric_scale']
      )) %}
    {% endif %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}


{% macro redshift__get_columns_in_relation_legacy(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      with bound_views as (
        select
          ordinal_position,
          table_schema,
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

        from information_schema."columns"
        where table_name = '{{ relation.identifier }}'
    ),

    unbound_views as (
      select
        ordinal_position,
        view_schema,
        col_name,
        case
          when col_type ilike 'character varying%' then
            'character varying'
          when col_type ilike 'numeric%' then 'numeric'
          else col_type
        end as col_type,
        case
          when col_type like 'character%'
          then nullif(REGEXP_SUBSTR(col_type, '[0-9]+'), '')::int
          else null
        end as character_maximum_length,
        case
          when col_type like 'numeric%'
          then nullif(
            SPLIT_PART(REGEXP_SUBSTR(col_type, '[0-9,]+'), ',', 1),
            '')::int
          else null
        end as numeric_precision,
        case
          when col_type like 'numeric%'
          then nullif(
            SPLIT_PART(REGEXP_SUBSTR(col_type, '[0-9,]+'), ',', 2),
            '')::int
          else null
        end as numeric_scale

      from pg_get_late_binding_view_cols()
      cols(view_schema name, view_name name, col_name name,
           col_type varchar, ordinal_position int)
      where view_name = '{{ relation.identifier }}'
    ),

    external_views as (
      select
        columnnum,
        schemaname,
        columnname,
        case
          when external_type ilike 'character varying%' or external_type ilike 'varchar%'
          then 'character varying'
          when external_type ilike 'numeric%' then 'numeric'
          else external_type
        end as external_type,
        case
          when external_type like 'character%' or external_type like 'varchar%'
          then nullif(
            REGEXP_SUBSTR(external_type, '[0-9]+'),
            '')::int
          else null
        end as character_maximum_length,
        case
          when external_type like 'numeric%'
          then nullif(
            SPLIT_PART(REGEXP_SUBSTR(external_type, '[0-9,]+'), ',', 1),
            '')::int
          else null
        end as numeric_precision,
        case
          when external_type like 'numeric%'
          then nullif(
            SPLIT_PART(REGEXP_SUBSTR(external_type, '[0-9,]+'), ',', 2),
            '')::int
          else null
        end as numeric_scale
      from
        pg_catalog.svv_external_columns
      where
        schemaname = '{{ relation.schema }}'
        and tablename = '{{ relation.identifier }}'

    ),

    unioned as (
      select * from bound_views
      union all
      select * from unbound_views
      union all
      select * from external_views
    )

    select
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale

    from unioned
    {% if relation.schema %}
    where table_schema = '{{ relation.schema }}'
    {% endif %}
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro redshift__list_relations_without_caching(schema_relation) %}

  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
        table_catalog as database,
        table_name as name,
        table_schema as schema,
        'table' as type
    from information_schema.tables
    where table_schema ilike '{{ schema_relation.schema }}'
    and table_type = 'BASE TABLE'
    union all
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case
        when view_definition ilike '%create materialized view%'
          then 'materialized_view'
        else 'view'
      end as type
    from information_schema.views
    where table_schema ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro redshift__information_schema_name(database) -%}
  {{ return(postgres__information_schema_name(database)) }}
{%- endmacro %}


{% macro redshift__list_schemas(database) %}
  {# DIVERGENCE: `adapter.has_feature` is Fusion-only; under dbt-core (1.x) short-circuit
     to postgres__list_schemas (matches upstream v1 behavior). Fusion path preserved
     unchanged so xdbc replay hashes stay stable. #}
  {%- if not dbt_version.startswith('2.') -%}{{ return(postgres__list_schemas(database)) }}{%- endif -%}
  {% if adapter.has_feature('datasharing') %}
    {# dbt-core's create_schemas passes a pre-quoted identifier via str(relation);
       other callers pass the raw database name. Only quote when the input is not
       already wrapped in double-quotes, so the identifier is quoted exactly once —
       required for databases with hyphens. #}
    {%- if database.startswith('"') and database.endswith('"') -%}
      {%- set _quoted_database = database -%}
    {%- else -%}
      {%- set _quoted_database = adapter.quote(database) -%}
    {%- endif -%}
    {% call statement('list_schemas', fetch_result=True) -%}
      SHOW SCHEMAS FROM DATABASE {{ _quoted_database }}
    {% endcall %}
    {{ return(load_result('list_schemas').table) }}
  {% else %}
    {{ return(postgres__list_schemas(database)) }}
  {% endif %}
{% endmacro %}

{% macro redshift__check_schema_exists(information_schema, schema) -%}
  {{ return(postgres__check_schema_exists(information_schema, schema)) }}
{%- endmacro %}


{% macro redshift__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {# Override: do not set column comments for LBVs #}
  {% set is_lbv = relation.type == 'view' and config.get('bind') == false %}
  {% if for_columns and config.persist_column_docs() and model.columns and not is_lbv %}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {% set filtered_columns = validate_doc_columns(relation, model.columns, existing_columns) %}
    {% set alter_comment_sql = alter_column_comment(relation, filtered_columns) %}
    {% if alter_comment_sql and alter_comment_sql | trim | length > 0 %}
      {% do run_query(alter_comment_sql) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{#
  Copied from the postgres-adapter.
#}
{% macro escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}

{% macro redshift__alter_relation_comment(relation, comment) %}
  {%- set escaped_comment = escape_comment(comment) -%}
  {%- set relation_type = 'view' if relation.type == 'materialized_view' else relation.type -%}
  comment on {{ relation_type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}


{% macro redshift__alter_column_comment(relation, column_dict) %}
  {% do return(postgres__alter_column_comment(relation, column_dict)) %}
{% endmacro %}


{% macro redshift__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns %}

    {% for column in add_columns %}
      {% set sql -%}
          alter {{ relation.type }} {{ relation }} add column {{ column.name }} {{ column.data_type }}
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}

  {% endif %}

  {% if remove_columns %}

    {% for column in remove_columns %}
      {% set sql -%}
          alter {{ relation.type }} {{ relation }} drop column {{ column.name }}
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}

  {% endif %}

{% endmacro %}
