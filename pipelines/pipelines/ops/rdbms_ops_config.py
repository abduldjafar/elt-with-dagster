def mysql_employees_tables_config():
    return [
        {
            "tb_name": "titles",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "salaries",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "employees",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "dept_manager",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "dept_emp_latest_date",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "dept_emp",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "current_dept_emp",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "departments",
            "db_destination": "dwh",
            "db_sources": "employees",
            "tb_columns": ["*"],
        },
    ]


def mysql_sakila_tables_config():
    return [
        
        {
            "tb_name": "actor",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "actor_info",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "address",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "category",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "city",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "country",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "customer",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "customer_list",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "film",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": [
                "film_id",
                "title",
                "description",
                "release_year",
                "language_id",
                "rental_duration",
                "rental_rate",
                "length",
                "rating",
                "special_features",
                "last_update",
            ],
        },
        {
            "tb_name": "film_actor",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "film_category",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "film_list",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "film_text",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "inventory",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "language",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "nicer_but_slower_film_list",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        
        {
            "tb_name": "payment",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "rental",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "sales_by_film_category",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "sales_by_store",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "staff",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "staff_list",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
        {
            "tb_name": "store",
            "db_destination": "dwh",
            "db_sources": "sakila",
            "tb_columns": ["*"],
        },
    ]
