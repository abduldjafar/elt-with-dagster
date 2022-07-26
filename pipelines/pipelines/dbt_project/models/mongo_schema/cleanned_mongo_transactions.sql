{{ config(order_by='account_id', engine='MergeTree()', materialized='table') }}

select
    account_id,
    bucket_start_date,
    bucket_end_date,
    transactions_date,
    transactions_amount,
    transaction_code,
    symbol,
    item_price,
    transaction_total
from (
    select
        JSONExtractInt(datas,'account_id') as account_id,
        parseDateTimeBestEffortOrNull(JSONExtractString(datas,'bucket_start_date')) as bucket_start_date,
        parseDateTimeBestEffortOrNull(JSONExtractString(datas,'bucket_end_date')) as bucket_end_date,
        arrayJoin(JSONExtractArrayRaw(datas,'transactions')) as transactions,
        parseDateTimeBestEffortOrNull(JSONExtractString(transactions,'date')) as transactions_date,
        JSONExtractFloat(transactions,'amount') as transactions_amount,
        JSONExtractString(transactions,'transaction_code') as transaction_code,
        JSONExtractString(transactions,'symbol') as symbol,
        floor(toFloat32(JSONExtractString(transactions,'price')),2) as item_price,
        floor(toFloat32(JSONExtractString(transactions,'total')),2) as transaction_total
    from mongo_transactions
)