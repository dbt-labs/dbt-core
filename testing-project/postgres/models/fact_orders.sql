select 
    *
    ,round(order_total - (order_total/2)) as discount_total
from {{ref('fact_orders_source')}}