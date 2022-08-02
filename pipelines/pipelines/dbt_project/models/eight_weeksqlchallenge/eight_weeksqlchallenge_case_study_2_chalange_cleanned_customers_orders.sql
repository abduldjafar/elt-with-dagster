{{ config(order_by='order_id', engine='MergeTree()', materialized='table') }}

select 
  order_id, 
  customer_id, 
  pizza_id, 
  toFloat32(
    replace(
      replaceOne(
        if(
          exclusions = 'null', 
          '0', 
          if(exclusions = '', '0', exclusions)
        ), 
        ',', 
        '.'
      ), 
      ' ', 
      ''
    )
  ) as exclusions, 
  toFloat32(
    replace(
      replaceOne(
        if(
          extras = 'null', 
          '0', 
          if(extras = '', '0', extras)
        ), 
        ',', 
        '.'
      ), 
      ' ', 
      ''
    )
  ) as extras, 
  order_time 
from 
  pizza_runner_customer_orders
