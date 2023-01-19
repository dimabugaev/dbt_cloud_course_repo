with customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from {{ref('stg_orders')}}

    group by 1

),
payments as (
    select 
        o.customer_id as customer_id,
        sum(case when p.status = 'success' then p.amount end) as lifetime_value
    from {{ref('stg_orders')}} o 
        left join {{ref('stg_payments')}} p on o.order_id = p.order_id
    group by 
        o.customer_id
        
),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(payments.lifetime_value, 0) as lifetime_value
    from {{ref('stg_customers')}} customers

    left join customer_orders using (customer_id)
    left join payments using (customer_id)

)

select * from final