# CDP Application Monorepo

## Backend file structure
- new_db_setup.py: Python script to create the database, tables and populates with dummy data. Currently uses postgres through cloud sql
- db_setup_queries.py: SQL queries to create the source tables
- [To be tested] cdp_procedure.py: Create a stored procedure to create the CDP table and calls the procedure
- [To be tested] backend.py: FastAPI backend to serve the dashboard
- [To be tested] backend_logic.py: Aggregation queries to serve the widgets in the dashboard
- [Not implemented] terraform: Terraform scripts to deploy the database and backend
- [Not implemented] docker: Dockerfile to containerize the backend
- [Not implemented]test_cdp.py: Tests on the CDP pipeline
- ddl_gen.py: Python script to generate the DDLs for the source tables

## List of dashboard elements:

1. Key Performance Indicators (KPIs):
   - Total Customer Count
   - Total Lifetime Value
   - Average Order Value
   - Customer Retention Rate
2. Customer Segment Distribution (Pie Chart)
3. Monthly Revenue Trend (Line Chart)
4. Top 5 Customers by Lifetime Value (Table)
5. Product Category Performance (Bar Chart)
6. Customer Satisfaction Score (Gauge Chart)
7. Churn Risk Distribution (Pie Chart)
8. RFM (Recency, Frequency, Monetary) Segmentation (Scatter Plot)


## Source Tables:

1. Customer Information:
```
customer_info
- customer_id (primary key)
- first_name
- last_name
- email
- phone_number
- date_of_birth
- registration_date
```

2. Purchase Transactions:
```
purchase_transactions
- transaction_id (primary key)
- customer_id (foreign key)
- product_id (foreign key)
- purchase_date
- quantity
- total_amount
- store_id
```

3. Product Catalog:
```
product_catalog
- product_id (primary key)
- product_name
- category
- brand
- price
- launch_date
```

4. Customer Service Interactions:
```
customer_service
- interaction_id (primary key)
- customer_id (foreign key)
- interaction_date
- interaction_type (e.g., complaint, inquiry, feedback)
- product_id (foreign key)
- resolution_status
- satisfaction_score
```

5. Marketing Campaigns:
```
marketing_campaigns
- campaign_id (primary key)
- campaign_name
- start_date
- end_date
- channel (e.g., email, social media, TV)
- target_audience
```

6. Campaign Responses:
```
campaign_responses
- response_id (primary key)
- campaign_id (foreign key)
- customer_id (foreign key)
- response_date
- response_type (e.g., click, purchase, unsubscribe)
```

7. Website Behavior:
```
website_behavior
- session_id (primary key)
- customer_id (foreign key)
- visit_date
- pages_viewed
- time_spent
- source (e.g., organic search, paid ad, direct)
```

## CDP Schema:

```
customer_360
- customer_id (primary key)
- first_name
- last_name
- email
- phone_number
- date_of_birth
- registration_date
- total_lifetime_value
- total_purchases
- last_purchase_date
- favorite_product_category
- favorite_brand
- average_order_value
- customer_segment
- recency_score
- frequency_score
- monetary_score
- last_interaction_date
- last_interaction_type
- average_satisfaction_score
- total_website_visits
- average_time_spent_on_site
- most_viewed_product_category
- campaign_response_rate
- preferred_marketing_channel
- churn_risk_score
```
