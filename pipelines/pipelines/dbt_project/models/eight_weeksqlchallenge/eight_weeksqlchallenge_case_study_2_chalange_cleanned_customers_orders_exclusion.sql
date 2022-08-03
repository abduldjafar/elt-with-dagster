{{ config(order_by='order_id', engine='MergeTree()', materialized='table') }}
select 
  distinct order_id, 
  if(
    temp_exclusions = '' 
    or temp_exclusions = 'null', 
    null, 
    temp_exclusions
  ) as exclusions 
from 
  (
    select 
      order_id, 
      arrayJoin(
        if(
          length(
            extractAll(t1.exclusions, '\d+')
          ) > 1, 
          extractAll(t1.exclusions, '\d+'), 
          array(t1.exclusions)
        )
      ) as temp_exclusions 
    from 
      pizza_runner_customer_orders as t1
  )
