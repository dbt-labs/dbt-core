
{{ config(materialized=var('materialized')) }}

select '{{ var("materialized") }}' as materialization

{% if is_incremental() %}
    where TRUE
{% endif %}
