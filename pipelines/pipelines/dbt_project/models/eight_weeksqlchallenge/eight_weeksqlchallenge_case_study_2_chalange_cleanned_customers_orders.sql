{{ config(order_by='order_id', engine='MergeTree()', materialized='table') }}
select 
  toInt8(order_id) as order_id, 
  toInt8(customer_id) as customer_id, 
  toInt8(pizza_id) as pizza_id,
  order_time 
from 
  pizza_runner_customer_orders
