with customers_orders as (
    select * from {{ ref('eight_weeksqlchallenge_case_study_2_chalange_cleanned_customers_orders')}}
),
cleanned_reciepes as (
    select * from {{ ref('eight_weeksqlchallenge_case_study_2_chalange_cleanned_reciepes')}}
),
runner_orders as (
    select * from {{ ref('eight_weeksqlchallenge_case_study_2_chalange_cleanned_runner_orders')}}
),
runner as(
    select * from pizza_runner_runners
),
topping as (
    select * from pizza_runner_pizza_toppings

),
pizza_name as (
    select * from pizza_runner_pizza_runner_pizza_names

),
extras as (
    select * from {{ ref('eight_weeksqlchallenge_case_study_2_chalange_cleanned_customers_orders_extras')}}
),
exclusions as (
    select * from {{ ref('eight_weeksqlchallenge_case_study_2_chalange_cleanned_customers_orders_exclusion')}}
)

select * from customers_orders