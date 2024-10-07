# CDP Application Monorepo

## Project Overview
This monorepo contains a Customer Data Platform (CDP) application, including a FastAPI backend, database setup scripts, and infrastructure configurations. The project aims to provide a comprehensive solution for managing customer data, analytics, and insights.

## Repository Structure
```
innovation_day_event_monorepo/
├── backend/
├── cdp/
├── docs/
├── frontend/
├── docker-compose.yml
├── main.tf
├── pyproject.toml
└── README.md
```

## Backend
The backend is built using FastAPI and provides API endpoints for accessing CDP data and analytics.

### Setup and Running
1. Navigate to the `backend` directory
2. Install dependencies: `pip install -r requirements.txt`
3. Run the server: `uvicorn main:app --host 0.0.0.0 --port 8000`

### API Endpoints
- `/`: Welcome message
- `/kpis`: Key Performance Indicators
- `/customer_segments`: Customer segment distribution
- `/monthly_revenue`: Monthly revenue trend
- `/top_customers`: Top 5 customers by lifetime value
- `/product_category_performance`: Product category performance
- `/customer_satisfaction`: Customer satisfaction score
- `/churn_risk`: Churn risk distribution
- `/rfm_segmentation`: RFM (Recency, Frequency, Monetary) segmentation

## CDP (Customer Data Platform)
The CDP component handles database setup, data initialization, and CDP procedure creation.

### CDP Procedure
The CDP procedure is defined in `cdp_procedure.py`. It creates the customer_360 table, which provides a comprehensive view of customer data.

## Database Schema Design

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

### Interacting with the Database
The application uses SQLAlchemy with asyncpg for asynchronous database operations. To interact with the database:

1. Ensure you have the necessary environment variables set (DB_USER, DB_PASS, DB_NAME, INSTANCE_CONNECTION_NAME)
2. Use the `get_db_session()` function in `backend_logic.py` to obtain a database session
3. Use SQLAlchemy's text() function to write raw SQL queries or use SQLAlchemy's ORM for more complex operations

Example:
```python
async with await get_db_session() as session:
    result = await session.execute(text("SELECT * FROM customer_360 LIMIT 10"))
    customers = result.fetchall()
```

## Frontend
(Placeholder for future frontend development)

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



## Development Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.9+
- Node.js 14+ (for frontend development)

### Local Development with Docker Compose
1. Clone the repository
2. Run `docker-compose up --build` in the root directory
3. Access the backend at `http://localhost:8000` and the frontend at `http://localhost:80`

## Deployment

### Google Cloud Deployment with Terraform
1. Install Terraform
2. Update the `main.tf` file with your Google Cloud project ID
3. Run `terraform init` and `terraform apply`

## Documentation
Additional documentation can be found in the `docs` directory:
- `app_requirements_doc.md`: Application requirements document
- `cdp_schema_design.md`: CDP schema design document

## Contributing
1. Fork the repository
2. Create a new branch for your feature
3. Commit your changes
4. Push to your branch
5. Create a pull request
