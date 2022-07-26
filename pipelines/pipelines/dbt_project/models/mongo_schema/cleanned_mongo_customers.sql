{{ config(order_by='accounts', engine='MergeTree()', materialized='table') }}

select
    JSONExtractString(datas,'username') as username,
    JSONExtractString(datas,'name') as name,
    JSONExtractString(datas,'address') as address,
    JSONExtractString(datas,'email') as email,
    parseDateTimeBestEffortOrNull(JSONExtractString(datas,'birthdate') ) as birthdate,
    replaceAll(arrayJoin(JSONExtractArrayRaw(datas,'accounts')),'"','') as accounts
from mongo_customers