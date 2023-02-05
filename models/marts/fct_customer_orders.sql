WITH base_orders as (
    select * from {{ source('jaffle_shop', 'orders') }}

),
base_customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),
base_payment as (
    select * from {{ source('stripe', 'payment') }}
),
paid_orders as (select base_orders.ID as order_id,
    base_orders.USER_ID	as customer_id,
    base_orders.ORDER_DATE AS order_placed_at,
        base_orders.STATUS AS order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.FIRST_NAME    as customer_first_name,
        C.LAST_NAME as customer_last_name
FROM base_orders
left join (select ORDERID as order_id, max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid
        from base_payment
        where STATUS <> 'fail'
        group by 1) p ON base_orders.ID = p.order_id
left join base_customers C on base_orders.USER_ID = C.ID )

select * from paid_orders