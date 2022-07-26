{{ config(materialized = 'view') }} 
select 
  s.store_id as store_id, 
  a.address_id as address_id, 
  a.city_id as city_id, 
  a.address as address, 
  a.address2 as address2, 
  a.district as district, 
  a.postal_code as postal_code, 
  a.phone as phone, 
  c.city as city, 
  cr.country as country 
from 
  sakila_store as s 
  inner join sakila_address as a on s.address_id = a.address_id 
  inner join sakila_city as c on a.city_id = c.city_id 
  inner join sakila_country as cr on c.country_id = cr.country_id