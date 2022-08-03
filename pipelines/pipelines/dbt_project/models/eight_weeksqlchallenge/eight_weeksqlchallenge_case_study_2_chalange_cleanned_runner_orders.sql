{{ config(order_by='runner_id', engine='MergeTree()', materialized='table') }}

with pre_cleanned_datas as (
select 
  order_id,
  runner_id, 
  distance_cleanned_in_km, 
  duration_cleanned_in_minutes, 
  cleanned_cancelation 
from 
  (
    select 
      order_id,
      runner_id, 
      cancellation, 
      extractAll(distance, '\d+') as distance_extracted, 
      extractAll(duration, '\d+') as duration_extracted, 
      if(
        length(distance_extracted) = 2, 
        concat(
          distance_extracted[1], '.', distance_extracted[2]
        ), 
        distance_extracted[1]
      ) as distance_cleanned_in_km, 
      if(
        length(duration_extracted) = 2, 
        concat(
          duration_extracted[1], '.', duration_extracted[2]
        ), 
        duration_extracted[1]
      ) as duration_cleanned_in_minutes, 
      parseDateTimeBestEffortOrNull(pickup_time), 
      if(
        cancellation = 'null', '', cancellation
      ) as cleanned_cancelation 
    from 
      pizza_runner_runner_orders
  )
 )
,
converted_data as (
     select 
        toInt8(order_id) as order_id,
        toInt8(runner_id) as runner_id,
        toFloat32(if(distance_cleanned_in_km ='','0',distance_cleanned_in_km))  as distance_cleanned_in_km,
        toFloat32(if(duration_cleanned_in_minutes ='','0',duration_cleanned_in_minutes))  as duration_cleanned_in_minutes,
        cleanned_cancelation
     from pre_cleanned_datas
)

select * from converted_data
