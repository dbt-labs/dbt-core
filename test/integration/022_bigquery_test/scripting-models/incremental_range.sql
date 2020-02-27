
{{
    config(
        materialized="incremental",
        unique_key="id",
        cluster_by="id",
        partition_by={
            "field": "id",
            "data_type": "int64",
            "range": {
                "start": 1,
                "end": 10,
                "interval": 1
            }
        }
    )
}}


with data as (
    select 1 as id, current_date() as ts union all
    select 2 as id, current_date() as ts union all
    select 3 as id, current_date() as ts union all
    select 4 as id, current_date() as ts

    {% if is_incremental() %}
        union all
        select 5 as id, current_date() as ts union all
        select 6 as id, current_date() as ts union all
        select 7 as id, current_date() as ts union all
        select 8 as id, current_date() as ts
    {% endif %}
)

select * from data

{% if is_incremental() %}
where id > _dbt_max_partition
{% endif %}
