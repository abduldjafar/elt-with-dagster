{{ config(order_by='order_id', engine='MergeTree()', materialized='table') }}
select 
  distinct order_id, 
  if(
    temp_extras = '' 
    or temp_extras = 'null', 
    null, 
    temp_extras
  ) as extras 
from 
  (
    select 
      order_id, 
      arrayJoin(
        if(
          length(
            extractAll(t1.extras, '\d+')
          ) > 1, 
          extractAll(t1.extras, '\d+'), 
          array(t1.extras)
        )
      ) as temp_extras 
    from 
      pizza_runner_customer_orders as t1
  )
