{#
  Batched (re)build of incremental models: the `reload_strategy` config.

      reload_strategy={'type': 'hash' | 'time_range' | 'value_range', 'column': '<column>', 'batches': 10}

  When the incremental materialization builds such a model from scratch (first run or
  --full-refresh), it creates the table empty and calls reload_in_batches, which fills it with one
  `insert ... select * from (<model sql>) where <batch filter>` per batch. A batch needs far less
  memory than one insert of the whole model, and the usual intermediate-table swap still only
  replaces the live table once every batch is in.

  - hash:        a hash of <column> modulo the number of batches (high-cardinality keys).
  - time_range:  equal time ranges between min and max of <column> (event timestamps).
  - value_range: ranges of whole <column> values with about the same number of rows per batch, so
                 a value never spans two batches (e.g. snapshot dates).
  Rows where <column> is null go to batch 1, so every row lands in exactly one batch.

  Extension points, overridable by adapters and projects:
  - reload_in_batches:                         how the batches are loaded.
  - get_reload_plan, get_reload_batch_filter:  strategy types.
  - get_reload_hash_bucket_sql:                the adapter's hash, used by `hash` and split retries.
#}

{% macro reload_in_batches(relation, sql, batches=none) -%}
  {{ return(adapter.dispatch('reload_in_batches', 'dbt')(relation, sql, batches)) }}
{%- endmacro %}

{% macro default__reload_in_batches(relation, sql, batches=none) %}
    {%- set strategy = config.get('reload_strategy') -%}
    {%- set batches = (batches or strategy.get('batches') or 10) | int -%}
    {%- set plan = get_reload_plan(strategy, sql, batches) -%}
    {{ log("reload " ~ relation ~ ": " ~ strategy['type'] ~ " on " ~ strategy['column'] ~ ", " ~ plan['rows'] ~ " rows in " ~ batches ~ " batches", info=True) }}
    {%- for batch in range(1, batches + 1) -%}
        {%- do reload_insert_batch(relation, sql, strategy, plan, batch, batches) -%}
    {%- endfor -%}
    {%- do reload_check_row_count(relation, plan['rows']) -%}
{% endmacro %}


{# Returns {'rows': <row count of the model's SQL>, 'bounds': [<first value of batch 2..N>]}.
   One aggregate query over the model's SQL. #}
{% macro get_reload_plan(strategy, sql, batches) -%}
  {{ return(adapter.dispatch('get_reload_plan', 'dbt')(strategy, sql, batches)) }}
{%- endmacro %}

{% macro default__get_reload_plan(strategy, sql, batches) %}
    {%- set column = strategy['column'] -%}
    {%- set bounds = [] -%}

    {%- if strategy['type'] == 'hash' -%}
        {%- set rows = reload_row_count('(' ~ sql ~ ') as _reload_src') -%}

    {%- elif strategy['type'] == 'time_range' -%}
        {%- set query -%}
            select
                row_count
                {%- for k in range(1, batches) %},
                {{ dbt.cast(dbt.dateadd('second', 'floor(' ~ dbt.datediff('lo', 'hi', 'second') ~ ' * ' ~ k ~ ' / ' ~ batches ~ ')', 'lo'), dbt.type_string()) }}
                {%- endfor %}
            from (
                select count(*) as row_count, min({{ column }}) as lo, max({{ column }}) as hi
                from ({{ sql }}) as _reload_src
            ) as _reload_range
        {%- endset -%}
        {%- set row = run_query(query).rows[0] -%}
        {%- set rows = row[0] | int -%}
        {%- for k in range(1, batches) if row[k] is not none -%}
            {%- do bounds.append(row[k]) -%}
        {%- endfor -%}

    {%- elif strategy['type'] == 'value_range' -%}
        {#- Count the rows per value, cut the running total into `batches` equal parts and take the
            smallest value of every part. Nulls sort first so they end up in batch 1. -#}
        {%- set query -%}
            select
                {{ dbt.cast('min(value)', dbt.type_string()) }},
                {{ dbt.cast('sum(row_count)', dbt.type_bigint()) }}
            from (
                select
                    value,
                    row_count,
                    floor(
                        (sum(row_count) over (order by case when value is null then 0 else 1 end, value rows between unbounded preceding and current row) - row_count)
                        * {{ batches }} / sum(row_count) over ()
                    ) as batch_index
                from (
                    select {{ column }} as value, count(*) as row_count
                    from ({{ sql }}) as _reload_src
                    group by {{ column }}
                ) as _reload_values
            ) as _reload_batches
            group by batch_index
            order by batch_index
        {%- endset -%}
        {%- set ns = namespace(rows=0) -%}
        {%- for row in run_query(query).rows -%}
            {%- if not loop.first -%}{%- do bounds.append(row[0]) -%}{%- endif -%}
            {%- set ns.rows = ns.rows + (row[1] | int) -%}
        {%- endfor -%}
        {%- set rows = ns.rows -%}

    {%- else -%}
        {%- do exceptions.raise_compiler_error("Unknown reload_strategy type '" ~ strategy['type'] ~ "'. Built-in types are hash, time_range and value_range; add others by overriding get_reload_plan and get_reload_batch_filter.") -%}
    {%- endif -%}

    {{ return({'rows': rows, 'bounds': bounds}) }}
{% endmacro %}


{# WHERE clause for batch `batch` (1-based) of `batches`. `part` of `parts` splits it further, so
   an orchestrator can retry a batch that ran out of memory in smaller pieces. #}
{% macro get_reload_batch_filter(strategy, plan, batch, batches, part=0, parts=1) -%}
  {{ return(adapter.dispatch('get_reload_batch_filter', 'dbt')(strategy, plan, batch, batches, part, parts)) }}
{%- endmacro %}

{% macro default__get_reload_batch_filter(strategy, plan, batch, batches, part=0, parts=1) %}
    {%- set column = strategy['column'] -%}

    {%- if strategy['type'] == 'hash' -%}
        {#- bucket % (batches * parts) = (batch - 1) + part * batches  splits bucket batch - 1 of `batches` -#}
        {%- set filter = get_reload_hash_bucket_sql(column, batches * parts) ~ ' = ' ~ (batch - 1 + part * batches) -%}

    {%- elif strategy['type'] in ('time_range', 'value_range') -%}
        {%- set bounds = plan['bounds'] -%}
        {%- set conditions = [] -%}
        {%- if batch > (bounds | length) + 1 -%}
            {#- Fewer distinct values than batches: nothing left for this batch. -#}
            {%- do conditions.append('1 = 0') -%}
        {%- else -%}
            {%- if batch > 1 -%}{%- do conditions.append(column ~ ' >= ' ~ reload_literal(bounds[batch - 2])) -%}{%- endif -%}
            {%- if batch <= bounds | length -%}{%- do conditions.append(column ~ ' < ' ~ reload_literal(bounds[batch - 1])) -%}{%- endif -%}
        {%- endif -%}
        {%- set filter = (conditions | join(' and ')) if conditions else '1 = 1' -%}
        {%- if parts > 1 -%}
            {%- set filter = '(' ~ filter ~ ') and ' ~ get_reload_hash_bucket_sql(column, parts) ~ ' = ' ~ part -%}
        {%- endif -%}

    {%- else -%}
        {%- do exceptions.raise_compiler_error("Unknown reload_strategy type '" ~ strategy['type'] ~ "'. Built-in types are hash, time_range and value_range; add others by overriding get_reload_plan and get_reload_batch_filter.") -%}
    {%- endif -%}

    {%- if batch == 1 and part == 0 -%}
        {%- set filter = '(' ~ filter ~ ') or ' ~ column ~ ' is null' -%}
    {%- endif -%}
    {{ return(filter) }}
{% endmacro %}


{# Integer bucket 0..buckets-1 of `column`, based on a hash of its value. #}
{% macro get_reload_hash_bucket_sql(column, buckets) -%}
  {{ return(adapter.dispatch('get_reload_hash_bucket_sql', 'dbt')(column, buckets)) }}
{%- endmacro %}

{% macro default__get_reload_hash_bucket_sql(column, buckets) %}
    {%- do exceptions.raise_compiler_error("reload_strategy needs get_reload_hash_bucket_sql, which the " ~ adapter.type() ~ " adapter does not implement.") -%}
{% endmacro %}


{% macro reload_insert_batch(relation, sql, strategy, plan, batch, batches, parts=1) %}
    {%- set started = modules.datetime.datetime.now() -%}
    {%- set rows_before = reload_row_count(relation) -%}
    {{ log("reload " ~ relation ~ ": batch " ~ batch ~ "/" ~ batches ~ (" in " ~ parts ~ " parts" if parts > 1 else "") ~ " where " ~ get_reload_batch_filter(strategy, plan, batch, batches), info=True) }}
    {%- for part in range(parts) -%}
        {%- do run_query("insert into " ~ relation ~ " select * from (" ~ sql ~ ") as _reload_src where " ~ get_reload_batch_filter(strategy, plan, batch, batches, part, parts)) -%}
    {%- endfor -%}
    {%- set rows_after = reload_row_count(relation) -%}
    {%- set seconds = (modules.datetime.datetime.now() - started).total_seconds() | round(1) -%}
    {%- set percent = (rows_after * 100 // plan['rows']) if plan['rows'] else 100 -%}
    {{ log("reload " ~ relation ~ ": batch " ~ batch ~ "/" ~ batches ~ " loaded " ~ (rows_after - rows_before) ~ " rows in " ~ seconds ~ "s, " ~ rows_after ~ " of " ~ plan['rows'] ~ " rows done (" ~ percent ~ "%)", info=True) }}
{% endmacro %}

{# Fails unless `relation` holds exactly the rows the plan counted, e.g. when the source changed
   during the batches. The live table is not replaced in that case. #}
{% macro reload_check_row_count(relation, expected_rows) %}
    {%- set actual_rows = reload_row_count(relation) -%}
    {%- if actual_rows != expected_rows -%}
        {%- do exceptions.raise_compiler_error("reload " ~ relation ~ ": has " ~ actual_rows ~ " rows, but the model's SQL returned " ~ expected_rows ~ " when the reload started. Either a batch was not loaded or the source data changed during the reload; the live table was not replaced.") -%}
    {%- endif -%}
    {{ log("reload " ~ relation ~ ": all " ~ actual_rows ~ " rows loaded", info=True) }}
{% endmacro %}

{% macro reload_row_count(relation) %}
    {{ return(run_query("select count(*) from " ~ relation).rows[0][0] | int) }}
{% endmacro %}

{% macro reload_literal(value) %}
    {{ return("'" ~ escape_single_quotes(value | string) ~ "'") }}
{% endmacro %}
