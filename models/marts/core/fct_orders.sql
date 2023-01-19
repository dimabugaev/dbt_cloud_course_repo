with orders as (

    select
        customer_id,
        order_id

    from {{ref('stg_orders')}}

),
payments as (
    select 
        order_id,
        SUM(case when status = 'success' then amount end) as amount
    from {{ref('stg_payments')}}
    group by
        order_id    
)
select
    o.customer_id,
    o.order_id,
    nvl(p.amount, 0) amount
from orders o
    left join payments p on o.order_id = p.order_id   