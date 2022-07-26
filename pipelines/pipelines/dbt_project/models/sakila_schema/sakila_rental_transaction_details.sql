{{ config(order_by='film_id', engine='MergeTree()', materialized='table') }}

select 
  f.film_id as film_id, 
  i.inventory_id as inventory_id, 
  i.store_id as store_id, 
  r.rental_id as rental_id, 
  p.amount as payment_amount, 
  r.rental_date as rental_date, 
  r.return_date as return_date, 
  p.payment_date as payment_date 
from 
  sakila_film as f 
  left join sakila_inventory as i on f.film_id = i.film_id 
  left join sakila_rental as r on i.inventory_id = r.inventory_id 
  left join sakila_payment as p on r.rental_id = p.rental_id