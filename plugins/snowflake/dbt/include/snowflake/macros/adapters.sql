{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {%- set transient = config.get('transient', default=true) -%}
  {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
  {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}

  {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_keys = [cluster_by_keys] -%}
  {%- endif -%}
  {%- if cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
  {% else %}
    {%- set cluster_by_string = none -%}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

      create or replace {% if temporary -%}
        temporary
      {%- elif transient -%}
        transient
      {%- endif %} table {{ relation }} {% if copy_grants and not temporary -%} copy grants {%- endif %} as
      (
        {%- if cluster_by_string is not none -%}
          select * from(
            {{ sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
          {{ sql }}
        {%- endif %}
      );
    {% if cluster_by_string is not none and not temporary -%}
      alter table {{relation}} cluster by ({{cluster_by_string}});
    {%- endif -%}
    {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
      alter table {{relation}} resume recluster;
    {%- endif -%}
{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  {%- set secure = config.get('secure', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create or replace {% if secure -%}
    secure
  {%- endif %} view {{ relation }} {% if copy_grants -%} copy grants {%- endif %} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__get_columns_in_relation(relation) -%}
  {%- set sql -%}
    describe table {{ relation }}
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation {{ relation }}! dbt can only get
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


{% macro snowflake__list_relations_without_caching(schema_relation) %}
  {%- set sql -%}
    show terse objects in {{ schema_relation }}
  {%- endset -%}

  {%- set result = run_query(sql) -%}
  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many schemas in schema  {{ schema_relation }}! dbt can only get
      information about schemas with fewer than {{ maximum }} objects.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {%- do return(result) -%}
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

{% macro snowflake__current_timestamp() -%}
  convert_timezone('UTC', current_timestamp())
{%- endmacro %}


{% macro snowflake__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp_ntz('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}


{% macro snowflake__snapshot_get_time() -%}
  to_timestamp_ntz({{ current_timestamp() }})
{%- endmacro %}


{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro snowflake__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter {{ adapter.quote(column_name) }} set data type {{ new_column_type }};
  {% endcall %}
{% endmacro %}

{% macro snowflake__alter_relation_comment(relation, relation_comment) -%}
  comment on {{ relation.type }} {{ relation }} IS $${{ relation_comment | replace('$', '[$]') }}$$;
{% endmacro %}


{% macro snowflake__alter_column_comment(relation, column_dict) -%}
    alter {{ relation.type }} {{ relation }} alter
    {% for column_name in column_dict %}
        {{ column_name }} COMMENT $${{ column_dict[column_name]['description'] | replace('$', '[$]') }}$$ {{ ',' if not loop.last else ';' }}
    {% endfor %}
{% endmacro %}
