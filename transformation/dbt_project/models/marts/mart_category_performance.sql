{{
  config(
    materialized='table'
  )
}}

select
    p.product_category_name,
    count(distinct oi.order_id) as total_orders,
    sum(oi.order_item_id) as total_items_sold,
    sum(oi.total_item_value) as total_revenue,
    avg(oi.price) as average_item_price
from {{ ref('fct_order_items') }} oi
join {{ ref('dim_products') }} p
    on oi.product_id = p.product_id
group by p.product_category_name
