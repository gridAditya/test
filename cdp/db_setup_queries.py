table_schema_init_queries = {
    "customer_info": """CREATE TABLE IF NOT EXISTS customer_info (
        customer_id SERIAL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        date_of_birth DATE,
        registration_date DATE
    );""",
    "product_catalog": """CREATE TABLE IF NOT EXISTS product_catalog (
        product_id SERIAL PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        brand TEXT,
        price REAL,
        launch_date DATE
    );""",
    "purchase_transactions": """CREATE TABLE IF NOT EXISTS purchase_transactions (
        transaction_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        product_id INTEGER,
        purchase_date DATE,
        quantity INTEGER,
        total_amount REAL,
        store_id INTEGER,
        FOREIGN KEY (customer_id) REFERENCES customer_info (customer_id),
        FOREIGN KEY (product_id) REFERENCES product_catalog (product_id)
    );""",
    "customer_service": """CREATE TABLE IF NOT EXISTS customer_service (
        interaction_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        interaction_date DATE,
        interaction_type TEXT,
        product_id INTEGER,
        resolution_status TEXT,
        satisfaction_score INTEGER,
        FOREIGN KEY (customer_id) REFERENCES customer_info (customer_id),
        FOREIGN KEY (product_id) REFERENCES product_catalog (product_id)
    );""",
    "marketing_campaigns": """CREATE TABLE IF NOT EXISTS marketing_campaigns (
        campaign_id SERIAL PRIMARY KEY,
        campaign_name TEXT,
        start_date DATE,
        end_date DATE,
        channel TEXT,
        target_audience TEXT
    );""",
    "campaign_responses": """
        CREATE TABLE IF NOT EXISTS campaign_responses (
        response_id SERIAL PRIMARY KEY,
        campaign_id INTEGER,
        customer_id INTEGER,
        response_date DATE,
        response_type TEXT,
        FOREIGN KEY (campaign_id) REFERENCES marketing_campaigns (campaign_id),
        FOREIGN KEY (customer_id) REFERENCES customer_info (customer_id)
    );""",
    "website_behavior": """CREATE TABLE IF NOT EXISTS website_behavior (
        session_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        visit_date DATE,
        pages_viewed INTEGER,
        time_spent INTEGER,
        source TEXT,
        FOREIGN KEY (customer_id) REFERENCES customer_info (customer_id)
    );""",
}
