with orders as (

    select * from {{ ref('fact_orders') }}

)
,
customers as (

    select * from {{ ref('dim_customers') }}

)
,
final as (

    select *
    from orders
    left join customers using (customer_id)

)

select * from final