{{ config(materialized = 'view') }} 

with main_table as (
  select 
    * 
  from 
    {{ ref('eight_weeksqlchallenge_case_study_1_fact_table') }}
), 
-- What is the total amount each customer spent at the restaurant?
total_amount_each_customer as (
  select 
    customer_id, 
    sum(price) as total_amount 
  from 
    main_table as e1 
  group by 
    customer_id 
  order by 
    customer_id
), 
-- How many days has each customer visited the restaurant?
days_each_customer_visited as (
  select 
    customer_id, 
    count(customer_id) as count_visited 
  from 
    (
      select 
        distinct customer_id, 
        order_date 
      from 
        main_table
    ) 
  group by 
    customer_id 
  order by 
    customer_id
), 
-- What was the first item from the menu purchased by each customer?
first_item_purchased as (
  select 
    distinct customer_id, 
    first_item_purchased 
  from 
    (
      select 
        customer_id, 
        order_date, 
        product_name as first_item_purchased, 
        rank() over (
          partition by customer_id 
          order by 
            order_date asc, 
            product_name
        ) as ranked 
      from 
        main_table
    ) 
  where 
    ranked = 1
), 
-- What is the most purchased item on the menu and how many times was it purchased by all customers?
most_buy as(
  SELECT 
    product_name 
  from 
    (
      SELECT 
        product_name, 
        count(product_name) as total_buy 
      from 
        main_table 
      GROUP by 
        product_name 
      order by 
        total_buy desc 
      limit 
        1
    )
), 
total_buy_best_product as (
  SELECT 
    customer_id, 
    count(product_name) total_buy_best_prdct 
  from 
    main_table 
  where 
    product_name = (
      SELECT 
        product_name 
      from 
        most_buy
    ) 
  GROUP by 
    customer_id 
  order by 
    customer_id
), 
-- Which item was the most popular for each customer?
populer_item_each_cust as (
    select distinct customer_id, product_name as most_populer_product_by_cust from (
            select
        customer_id,
        product_name,
        count(product_name) as total_buyed,
        row_number() over (
            partition by customer_id
            order by total_buyed desc
        ) as ranked
    from main_table
    group by customer_id,product_name
    )
    where ranked = 1
    
),
-- Which item was purchased first by the customer after they became a member?
filtered_get_joinned_date as (
select 
  m.customer_id as customer_id,
  mb.join_date,
  m.order_date,
  m.product_name
  
from 
  main_table as m 
  inner join learn_members as mb on m.customer_id = mb.customer_id 
where 
  m.order_date >= mb.join_date
),
clean_filtered_get_joinned_date as (
    
    select customer_id,product_name from (
        select 
            t.* ,
            rank() over (
                partition by customer_id
                order by order_date 
            ) as ranked
        from 
            filtered_get_joinned_date as t
    )
    where ranked = 1

),

-- resume all tasks
resume_all as (
  select 
    t1.customer_id as customer_id, 
    t1.total_amount as total_amount, 
    t2.count_visited as count_visited, 
    t3.first_item_purchased as first_item_purchased, 
    most_buy.product_name as best_product, 
    t4.total_buy_best_prdct as total_buy_best_product ,
    t5.most_populer_product_by_cust as most_populer_product_by_cust,
    t6.product_name as first_item_purchased_after_became_member
  from 
    total_amount_each_customer as t1 
    inner join days_each_customer_visited as t2 on t1.customer_id = t2.customer_id 
    inner join first_item_purchased as t3 on t1.customer_id = t3.customer_id 
    inner join total_buy_best_product as t4 on t1.customer_id = t4.customer_id
    inner join populer_item_each_cust as t5 on t1.customer_id = t5.customer_id
    left join clean_filtered_get_joinned_date as t6 on t1.customer_id = t6.customer_id
    , 
    most_buy
) 
select 
  * 
from 
  resume_all
