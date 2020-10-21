

{{
    config(
        materialized="table",
        partition_by=var('partition_by'),
        cluster_by=var('cluster_by')
    )
}}

select 1 as id, 'dr. bigquery' as name, current_timestamp() as cur_time, current_date() as cur_date
union all
select 2 as id, 'prof. bigquery' as name, current_timestamp() as cur_time, current_date() as cur_date
