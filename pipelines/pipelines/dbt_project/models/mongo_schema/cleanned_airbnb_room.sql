{{ config(materialized = 'view') }} 

SELECT 
    JSONExtractString(datas, '_id') as id,
    JSONExtractString(datas, 'listing_url') as listing_url,
    JSONExtractString(datas, 'name') as name,
    JSONExtractString(datas, 'summary') as summary,
    JSONExtractString(datas, 'interaction') as interaction,
    JSONExtractString(datas, 'house_rules') as house_rules,
    JSONExtractString(datas, 'property_type') as property_type,
    JSONExtractString(datas, 'room_type') as room_type,
    JSONExtractString(datas, 'bed_type') as bed_type,
    JSONExtractString(datas, 'minimum_nights') as minimum_nights,
    JSONExtractString(datas, 'maximum_nights') as maximum_nights,
    JSONExtractString(datas, 'cancellation_policy') as cancellation_policy,
    JSONExtractString(datas, 'last_scraped') as last_scraped,
    JSONExtractString(datas, 'calendar_last_scraped') as calendar_last_scraped,
    JSONExtractString(datas, 'first_review') as first_review,
    JSONExtractString(datas, 'last_review') as last_review,
    JSONExtractInt(datas, 'accommodates') as accommodates,
    JSONExtractInt(datas, 'bedrooms') as bedrooms,
    JSONExtractInt(datas, 'beds') as beds,
    JSONExtractInt(datas, 'number_of_reviews') as number_of_reviews
FROM mongo_listingsAndReviews