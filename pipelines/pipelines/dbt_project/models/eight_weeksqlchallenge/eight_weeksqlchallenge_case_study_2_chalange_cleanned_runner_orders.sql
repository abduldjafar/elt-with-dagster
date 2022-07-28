select 
  runner_id, 
  distance_cleanned_in_km, 
  duration_cleanned_in_minutes, 
  cleanned_cancelation 
from 
  (
    select 
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
        cancellation = 'null', null, cancellation
      ) as cleanned_cancelation 
    from 
      pizza_runner_runner_orders
  )
