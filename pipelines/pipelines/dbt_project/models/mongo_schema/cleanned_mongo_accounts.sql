{{ config(order_by='account_id', engine='MergeTree()', materialized='table') }}

SELECT 
    JSONExtractInt(datas, 'account_id') as account_id,
    JSONExtractInt(datas,'limit') as `limit`,
    replaceAll(arrayJoin(JSONExtractArrayRaw(datas,'products')),'"','') as products
FROM mongo_accounts