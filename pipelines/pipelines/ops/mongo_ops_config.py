def tables_config():
    return [
        {"collection_name": "listingsAndReviews", "db_name":"sample_airbnb","db_destination": "dwh"},
        {"collection_name": "accounts", "db_name":"sample_analytics","db_destination": "dwh"},
        {"collection_name": "customers", "db_name":"sample_analytics","db_destination": "dwh"},
        {"collection_name": "transactions", "db_name":"sample_analytics","db_destination": "dwh"}    ]
