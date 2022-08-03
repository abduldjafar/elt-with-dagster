{{ config(order_by='pizza_id', engine='MergeTree()', materialized='table') }}

select 
  toInt8(pizza_id) as pizza_id, 
  arrayJoin(
    extractAll(toppings, '\d+')
  ) as toppings 
from 
  pizza_runner_pizza_recipes
