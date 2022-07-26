{{ config(order_by='symbol', engine='MergeTree()', materialized='table') }}

with final_table as (
    select 
        symbol,
        transactions_date,
        year(transactions_date) as years,
        transaction_code,
        avg(item_price) over (
                PARTITION BY years,symbol,transaction_code 
                rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as average_item_price_in_year
    from {{ ref('cleanned_mongo_transactions') }}
    order by years desc
)
select * from final_table