{{ config(materialized = 'view') }} 
select 
  fa.film_id as film_id, 
  ai.actor_id as actor_id, 
  ai.first_name as actor_first_name, 
  ai.last_name as actor_last_name, 
  f.title as actor_in_film 
from 
  sakila_actor_info as ai 
  left join sakila_film_actor as fa on ai.actor_id = fa.actor_id 
  left join sakila_film as f on fa.film_id = f.film_id 
order by 
  film_id
