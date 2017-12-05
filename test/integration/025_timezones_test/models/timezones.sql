
{{
    config(
        materialized='table'
    )
}}

select
    '{{ run_started_at.astimezone(modules.pytz.timezone("America/New_York")) }}' as run_started_at_est,
    '{{ run_started_at }}' as run_started_at_utc
