
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_int",
            "data_type": "int64",
            "range": {
                "start": 20200101,
                "end": 20200110,
                "interval": 1
            }
        }
    )
}}


with data as (
    
    {% if not is_incremental() %}
    
        select 1 as id, 20200101 as date_int union all
        select 2 as id, 20200101 as date_int union all
        select 3 as id, 20200101 as date_int union all
        select 4 as id, 20200101 as date_int

    {% else %}
        
        -- we want to overwrite the 4 records in the 20200101 partition
        -- with the 2 records below, but add two more in the 20200102 partition
        select 10 as id, 20200101 as date_int union all
        select 20 as id, 20200101 as date_int union all
        select 30 as id, 20200102 as date_int union all
        select 40 as id, 20200102 as date_int
    
    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_int >= _dbt_max_partition
{% endif %}
