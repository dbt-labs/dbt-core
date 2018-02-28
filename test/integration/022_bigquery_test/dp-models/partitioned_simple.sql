
{{
    config(
        materialized='table',
        partition_date='20180101',
        verbose=True
    )
}}

-- Hack to make sure our events models run first.
-- In practice, these would be source data
-- {{ ref('events_20180101') }}

select * from `{{ this.schema }}`.`events_20180101`
