
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_day",
            "data_type": "date"
        }
    )
}}


with data as (
    
    {% if not is_incremental() %}
    
        select 1 as id, cast('2020-01-01' as date) as date_day union all
        select 2 as id, cast('2020-01-01' as date) as date_day union all
        select 3 as id, cast('2020-01-01' as date) as date_day union all
        select 4 as id, cast('2020-01-01' as date) as date_day

    {% else %}
    
        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as date) as date_day union all
        select 20 as id, cast('2020-01-01' as date) as date_day union all
        select 30 as id, cast('2020-01-02' as date) as date_day union all
        select 40 as id, cast('2020-01-02' as date) as date_day
    
    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_day >= _dbt_max_partition
{% endif %}
