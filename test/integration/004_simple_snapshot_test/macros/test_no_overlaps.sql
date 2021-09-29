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
