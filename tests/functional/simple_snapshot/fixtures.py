snapshots_select__snapshot_sql = """
{% snapshot snapshot_castillo %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='"1-updated_at"',
        )
    }}
    select id,first_name,last_name,email,gender,ip_address,updated_at as "1-updated_at" from {{target.database}}.{{schema}}.seed where last_name = 'Castillo'

{% endsnapshot %}

{% snapshot snapshot_alvarez %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Alvarez'

{% endsnapshot %}


{% snapshot snapshot_kelly %}
    {# This has no target_database set, which is allowed! #}
    {{
        config(
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Kelly'

{% endsnapshot %}
"""

snapshots_pg_custom__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""


macros_custom_snapshot__custom_sql = """
{# A "custom" strategy that's really just the timestamp one #}
{% macro snapshot_custom_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
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
"""


models__schema_yml = """
version: 2
snapshots:
  - name: snapshot_actual
    tests:
      - mutually_exclusive_ranges
    config:
      meta:
        owner: 'a_owner'
"""

models__ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""

macros__test_no_overlaps_sql = """
{% macro get_snapshot_unique_id() -%}
    {{ return(adapter.dispatch('get_snapshot_unique_id')()) }}
{%- endmacro %}

{% macro default__get_snapshot_unique_id() -%}
  {% do return("id || '-' || first_name") %}
{%- endmacro %}

{#
    mostly copy+pasted from dbt_utils, but I removed some parameters and added
    a query that calls get_snapshot_unique_id
#}
{% test mutually_exclusive_ranges(model) %}

with base as (
    select {{ get_snapshot_unique_id() }} as dbt_unique_id,
    *
    from {{ model }}
),
window_functions as (

    select
        dbt_valid_from as lower_bound,
        coalesce(dbt_valid_to, '2099-1-1T00:00:01') as upper_bound,

        lead(dbt_valid_from) over (
            partition by dbt_unique_id
            order by dbt_valid_from
        ) as next_lower_bound,

        row_number() over (
            partition by dbt_unique_id
            order by dbt_valid_from desc
        ) = 1 as is_last_record

    from base

),

calc as (
    -- We want to return records where one of our assumptions fails, so we'll use
    -- the `not` function with `and` statements so we can write our assumptions nore cleanly
    select
        *,

        -- For each record: lower_bound should be < upper_bound.
        -- Coalesce it to return an error on the null case (implicit assumption
        -- these columns are not_null)
        coalesce(
            lower_bound < upper_bound,
            is_last_record
        ) as lower_bound_less_than_upper_bound,

        -- For each record: upper_bound {{ allow_gaps_operator }} the next lower_bound.
        -- Coalesce it to handle null cases for the last record.
        coalesce(
            upper_bound = next_lower_bound,
            is_last_record,
            false
        ) as upper_bound_equal_to_next_lower_bound

    from window_functions

),

validation_errors as (

    select
        *
    from calc

    where not(
        -- THE FOLLOWING SHOULD BE TRUE --
        lower_bound_less_than_upper_bound
        and upper_bound_equal_to_next_lower_bound
    )
)

select * from validation_errors
{% endtest %}
"""


snapshots_select_noconfig__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}

{% snapshot snapshot_castillo %}

    {{
        config(
            target_database=var('target_database', database),
            updated_at='"1-updated_at"',
        )
    }}
    select id,first_name,last_name,email,gender,ip_address,updated_at as "1-updated_at" from {{target.database}}.{{schema}}.seed where last_name = 'Castillo'

{% endsnapshot %}

{% snapshot snapshot_alvarez %}

    {{
        config(
            target_database=var('target_database', database),
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Alvarez'

{% endsnapshot %}


{% snapshot snapshot_kelly %}
    {# This has no target_database set, which is allowed! #}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Kelly'

{% endsnapshot %}
"""


seeds__seed_newcol_csv = """id,first_name,last_name
1,Judith,Kennedy
2,Arthur,Kelly
3,Rachel,Moreno
"""

seeds__seed_csv = """id,first_name
1,Judith
2,Arthur
3,Rachel
"""


snapshots_pg_custom_namespaced__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='test.custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""

snapshots_pg__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}

    {% if var('invalidate_hard_deletes', 'false') | as_bool %}
        {{ config(invalidate_hard_deletes=True) }}
    {% endif %}

    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""


models_slow__gen_sql = """

{{ config(materialized='ephemeral') }}


/*
    Generates 50 rows that "appear" to update every
    second to a query-er.

    1	2020-04-21 20:44:00-04	0
    2	2020-04-21 20:43:59-04	59
    3	2020-04-21 20:43:58-04	58
    4	2020-04-21 20:43:57-04	57

    .... 1 second later ....

    1	2020-04-21 20:44:01-04	1
    2	2020-04-21 20:44:00-04	0
    3	2020-04-21 20:43:59-04	59
    4	2020-04-21 20:43:58-04	58

    This view uses pg_sleep(2) to make queries against
    the view take a non-trivial amount of time

    Use statement_timestamp() as it changes during a transactions.
    If we used now() or current_time or similar, then the timestamp
    of the start of the transaction would be returned instead.
*/

with gen as (

    select
        id,
        date_trunc('second', statement_timestamp()) - (interval '1 second' * id) as updated_at

    from generate_series(1, 10) id

)

select
    id,
    updated_at,
    extract(seconds from updated_at)::int as seconds

from gen, pg_sleep(2)
"""

snapshots_longtext__snapshot_sql = """
{% snapshot snapshot_actual %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.super_long
{% endsnapshot %}
"""

snapshots_check_col_noconfig__snapshot_sql = """
{% snapshot snapshot_actual %}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}

{# This should be exactly the same #}
{% snapshot snapshot_checkall %}
    {{ config(check_cols='all') }}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}
"""
