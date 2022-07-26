{{ config(materialized = 'view') }} 

with store_count_datas as (
  select 
    film_id, 
    count(store_id) as store_count 
  from 
    (
      select 
        distinct(film_id), 
        store_id, 
        count(store_id) as store_count 
      from 
        sakila_inventory 
      group by 
        film_id, 
        store_id 
      order by 
        film_id
    ) 
  group by 
    film_id
), 
raws_datas_joined as (
  select 
    * 
  from 
    sakila_inventory as i 
    inner join sakila_rental as r on i.inventory_id = r.inventory_id 
    inner join sakila_payment as p on r.rental_id = p.rental_id 
    inner join sakila_customer as c on p.customer_id = c.customer_id
), 
get_store_that_sells_the_most as(
  select 
    film_id, 
    store_id as store_that_sells_the_most 
  from 
    (
      select 
        i.film_id as film_id, 
        c.store_id as store_id, 
        count(c.store_id) as store_that_sells_the_most, 
        row_number() OVER (
          PARTITION BY film_id 
          order by 
            store_that_sells_the_most desc
        ) AS row_numbers 
      from 
        raws_datas_joined 
      group by 
        film_id, 
        c.store_id 
      order by 
        film_id
    ) 
  where 
    row_numbers = 1
), 
get_most_sales_in_the_country as(
  select 
    film_id, 
    country_id, 
    payment_amount 
  from 
    (
      select 
        t.film_id, 
        s.country_id as country_id, 
        count(s.country_id) as most_sales_in_the_country, 
        sum(t.payment_amount) as payment_amount, 
        row_number() OVER (
          PARTITION BY film_id 
          order by 
            most_sales_in_the_country desc
        ) AS row_numbers 
      from 
        (
          select 
            rw.i.film_id as film_id, 
            add.city_id as city_id, 
            rw.p.amount as payment_amount 
          from 
            raws_datas_joined as rw 
            inner join sakila_address as add 
            on c.address_id = add.address_id
        ) as t 
        inner join sakila_city as s 
        on t.city_id = s.city_id 
      group by 
        t.film_id, 
        country_id 
      order by 
        film_id
    ) 
  where 
    row_numbers = 1
), 
total_rental_payment as (
  select 
    i.film_id as film_id, 
    sum(p.amount) as rental_payment_amount_from_all_over_the_world 
  from 
    raws_datas_joined 
    inner join get_most_sales_in_the_country as gt on gt.film_id = i.film_id 
  group by 
    i.film_id
), 
total_item_was_sell as(
  select 
    i.film_id as film_id, 
    count(p.payment_id) as total_item_was_sell_in_the_world 
  from 
    raws_datas_joined 
  group by 
    i.film_id
), 
final_result as (
  select
    f.film_id as film_id,
    s.store_count as number_of_store_that_rent, 
    g.store_that_sells_the_most as store_that_sells_the_most, 
    tr.rental_payment_amount_from_all_over_the_world as rental_payment_amount_from_all_over_the_world, 
    ts.total_item_was_sell_in_the_world as total_item_was_sell_in_the_world, 
    gm.payment_amount as payment_amount_in_populared_country, 
    sc.country as popular_in_country 
  from
    sakila_film as f
    left join store_count_datas as s on f.film_id = s.film_id
    left join get_store_that_sells_the_most as g on s.film_id = g.film_id 
    left join get_most_sales_in_the_country as gm on s.film_id = gm.film_id 
    left join sakila_country as sc on gm.country_id = sc.country_id 
    left join total_rental_payment as tr on s.film_id = tr.film_id 
    left join total_item_was_sell as ts on s.film_id = ts.film_id
) 
select 
  * 
from 
  final_result
