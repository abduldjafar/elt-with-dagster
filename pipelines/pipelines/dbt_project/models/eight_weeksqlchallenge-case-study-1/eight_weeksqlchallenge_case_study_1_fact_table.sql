{{ config(materialized = 'view') }} 

with joined_table as (
  select 
    s.customer_id as customer_id, 
    s.order_date as order_date, 
    p.product_name as product_name, 
    p.price as price, 
    m.join_date as join_date, 
    if(m.customer_id='',Null,m.customer_id) as member_id,
    if(
      (m.join_date > s.order_date) ,'N',if(isnull(member_id),'N','Y')
    ) as member
  from 
    learn_sales as s
    left join learn_members as m  on m.customer_id = s.customer_id 
    left join learn_menu as p on s.product_id = p.product_id
), 
ranked_table as (
  select 
    t.*, 
    rank() over (
      partition by t.customer_id, 
      member 
      order by 
        t.order_date, 
        t.price desc
    ) as ranked 
  from 
    joined_table as t
), 
cleanned_table as (
  select 
    customer_id, 
    order_date, 
    product_name, 
    price, 
    member, 
    if(member = 'N', Null, ranked) as ranking 
  from 
    ranked_table as t
) 
select 
  * 
from 
  cleanned_table