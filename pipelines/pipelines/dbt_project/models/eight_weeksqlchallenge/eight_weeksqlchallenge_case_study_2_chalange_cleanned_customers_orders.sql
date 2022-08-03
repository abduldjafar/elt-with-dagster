{{ config(order_by='order_id', engine='MergeTree()', materialized='table') }}
select 
  order_id, 
  customer_id, 
  pizza_id,
  order_time 
from 
  pizza_runner_customer_orders
