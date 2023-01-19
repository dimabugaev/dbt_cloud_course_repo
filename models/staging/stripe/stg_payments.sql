with payments as (

    select
        ID as payment_id, 
        ORDERID as order_id, 
        PAYMENTMETHOD as payment_method, 
        STATUS, 
        AMOUNT/100 as amount, 
        CREATED, 
        _BATCHED_AT as updated
        
    from {{source('stripe', 'payment')}}

)
select * from payments