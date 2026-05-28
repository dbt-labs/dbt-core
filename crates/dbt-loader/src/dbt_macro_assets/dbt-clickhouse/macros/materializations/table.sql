{% materialization table, adapter='clickhouse' %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# MVP: use CREATE OR REPLACE TABLE which is atomic in ClickHouse 22.9+.
     This handles new tables, existing tables, and full-refresh identically. #}
  {% call statement('main') -%}
    create or replace table {{ target_relation }}
    {{ on_cluster_clause(target_relation) }}
    {{ engine_clause() }}
    {{ order_cols(label='order by') }}
    {{ primary_key_clause(label='primary key') }}
    {{ partition_cols(label='partition by') }}
    {{ ttl_config(label='ttl') }}
    {{ clickhouse_model_settings(model, config.get('engine', default='MergeTree')) }}
    as (
      {{ sql }}
    )
    {{ clickhouse_model_query_settings(model) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro engine_clause() %}
  engine = {{ config.get('engine', default='MergeTree()') }}
{%- endmacro -%}

{#-
  Get all dbt-managed materialized views that point to a target table.
  Returns a list of dictionaries with MV info including the SELECT SQL.
  Used by table materialization for atomic full refresh with MV repopulation.

  This uses the cached relation data (mvs_pointing_to_it) which already contains
  {schema, name, sql} dicts for each MV. We filter to only include MVs that
  are also defined in the dbt project (to exclude non-dbt MVs).

  Note: On first run, MVs don't exist in ClickHouse yet, so repopulation
  won't happen (which is correct - there's no data to preserve on first run).
-#}
{% macro clickhouse__get_dbt_mvs_for_target(relation) %}
  {%- set dbt_mvs = [] -%}
  {%- if relation is none or relation.mvs_pointing_to_it | length == 0 -%}
    {{ return(dbt_mvs) }}
  {%- endif -%}

  {%- set seen_mvs = [] -%}
  {%- for mv in relation.mvs_pointing_to_it -%}
    {%- set mv_key = mv.schema ~ '.' ~ mv.name -%}
    {%- if mv_key not in seen_mvs -%}
      {#- Only include MVs that are also defined in dbt (to filter out non-dbt MVs) -#}
      {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == 'model'
            and node.config.materialized == 'materialized_view'
            and node.schema == mv.schema
            and node.alias == mv.name -%}
          {%- do dbt_mvs.append(mv) -%}
          {%- do seen_mvs.append(mv_key) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}

  {{ return(dbt_mvs) }}
{% endmacro %}

{% macro partition_cols(label) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro primary_key_clause(label) %}
  {%- set primary_key = config.get('primary_key', validator=validation.any[basestring]) -%}

  {%- if primary_key is not none %}
    {{ label }} {{ primary_key }}
  {%- endif %}
{%- endmacro -%}

{% macro order_cols(label) %}
  {%- set cols = config.get('order_by', validator=validation.any[list, basestring]) -%}
  {%- set engine = config.get('engine', default='MergeTree()') -%}
  {%- set supported = [
    'HDFS',
    'MaterializedPostgreSQL',
    'S3',
    'EmbeddedRocksDB',
    'Hive'
  ] -%}

  {%- if 'MergeTree' in engine or engine in supported %}
    {%- if cols is not none %}
      {%- if cols is string -%}
        {%- set cols = [cols] -%}
      {%- endif -%}
      {{ label }} (
      {%- for item in cols -%}
        {{ item }}
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
      )
    {%- else %}
      {{ label }} (tuple())
    {%- endif %}
  {%- endif %}
{%- endmacro -%}

{% macro ttl_config(label) %}
  {%- if config.get("ttl")%}
    {{ label }} {{ config.get("ttl") }}
  {%- endif %}
{%- endmacro -%}

{% macro on_cluster_clause(relation, force_sync) %}
  {% set active_cluster = get_clickhouse_cluster_name() %}
  {%- if active_cluster is not none and relation.should_on_cluster %}
    {# Add trailing whitespace to avoid problems when this clause is not last #}
    ON CLUSTER {{ active_cluster + ' ' }}
    {%- if force_sync %}
    SYNC
    {%- endif %}
  {%- endif %}
{%- endmacro -%}

{% macro clickhouse__create_table_as(temporary, relation, sql) -%}
    {% set has_contract = config.get('contract').enforced %}
    {% set create_table = create_table_or_empty(temporary, relation, sql, has_contract) %}
    {% if clickhouse_is_before_version('22.7.1.2484') or temporary -%}
        {{ create_table }}
    {%- else %}
        {% call statement('create_table_empty') %}
            {{ create_table }}
        {% endcall %}
         {{ add_index_and_projections(relation) }}

        {{ clickhouse__insert_into(relation, sql, has_contract) }}
    {%- endif %}
{%- endmacro %}

{#
    A macro that adds any configured projections or indexes at the same time.
    We optimise to reduce the number of ALTER TABLE statements that are run to avoid
    Code: 517.
    DB::Exception: Metadata on replica is not up to date with common metadata in Zookeeper.
    It means that this replica still not applied some of previous alters. Probably too many
    alters executing concurrently (highly not recommended).
#}
{% macro add_index_and_projections(relation) %}
    {%- set projections = config.get('projections', default=[]) -%}
    {%- set indexes = config.get('indexes', default=[]) -%}

    {% if projections | length > 0 or indexes | length > 0 %}
        {% call statement('add_projections_and_indexes') %}
            ALTER TABLE {{ relation }}
            {%- if projections %}
                {%- for projection in projections %}
                    ADD PROJECTION {{ projection.get('name') }} ({{ projection.get('query') }})
                    {%- if not loop.last or indexes | length > 0 -%}
                        ,
                    {% endif %}
                {%- endfor %}
            {%- endif %}
            {%- if indexes %}
                {%- for index in indexes %}
                    ADD INDEX {{ index.get('name') }} {{ index.get('definition') }}
                    {%- if not loop.last -%}
                        ,
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endcall %}
    {% endif %}
{% endmacro %}

{% macro create_table_or_empty(temporary, relation, sql, has_contract) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary -%}
        create temporary table {{ relation.identifier }}
        engine Memory
        {{ clickhouse_model_settings(model, 'Memory') }}
        as (
          {{ sql }}
        )
    {%- else %}
        create table {{ relation }}
        {{ on_cluster_clause(relation)}}
        {%- if has_contract%}
          {{ get_assert_columns_equivalent(sql) }}
          {{ get_table_columns_and_constraints() }}
        {%- endif %}
        {{ engine_clause() }}
        {{ order_cols(label="order by") }}
        {{ primary_key_clause(label="primary key") }}
        {{ partition_cols(label="partition by") }}
        {{ ttl_config(label="ttl")}}
        {{ clickhouse_model_settings(model, config.get('engine', default='MergeTree')) }}

        {%- if not has_contract %}
          {%- if not clickhouse_is_before_version('22.7.1.2484') %}
            empty
          {%- endif %}
          as (
            {{ sql }}
          )
        {%- endif %}
        {{ clickhouse_model_query_settings(model) }}
    {%- endif %}

{%- endmacro %}

{% macro clickhouse__insert_into(insert_relation, sql, has_contract, use_columns_from_sql=False) %}
  {% if use_columns_from_sql %}
    {% set dest_columns = clickhouse__get_columns_in_query(sql) %}
    {%- set ns = namespace(quoted_cols=[]) -%}
    {%- for col in dest_columns -%}
      {%- set ns.quoted_cols = ns.quoted_cols + [adapter.quote(col)] -%}
    {%- endfor -%}
    {%- set dest_cols_csv = ns.quoted_cols | join(', ') -%}
  {%- else %}
    {%- set dest_columns = adapter.get_columns_in_relation(insert_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  {%- endif %}

  insert into {{ insert_relation }}
        {% if dest_cols_csv %}({{ dest_cols_csv }}){% endif %}
  {%- if has_contract -%}
    -- Use a subquery to get columns in the right order
          SELECT {{ dest_cols_csv }}
          FROM (
            {{ sql }}
            )
  {%- else -%}
      {{ sql }}
  {%- endif -%}
  {{ clickhouse_model_query_settings(model) }}
{%- endmacro %}

{% macro codec_clause(codec_name) %}
  {%- if codec_name %}
      CODEC({{ codec_name }})
  {%- endif %}
{% endmacro %}
