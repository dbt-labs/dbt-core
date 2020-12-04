

{{
    config(
        materialized="table",
        partition_by=var('partition_by'),
        cluster_by=var('cluster_by'),
        partition_expiration_days=var('partition_expiration_days'),
        require_partition_filter=var('require_partition_filter')
    )
}}

select 1 as id, 'dr. bigquery' as name, current_timestamp() as cur_time, current_date() as cur_date
union all
select 2 as id, 'prof. bigquery' as name, current_timestamp() as cur_time, current_date() as cur_date
