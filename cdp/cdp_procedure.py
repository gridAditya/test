import os
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from google.cloud.sql.connector import create_async_connector
from dotenv import load_dotenv

load_dotenv()  # load environment variables


async def init_connection_pool(connector):
    async def getconn():
        conn = await connector.connect_async(
            os.environ["INSTANCE_CONNECTION_NAME"],
            "asyncpg",
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            db=os.environ["DB_NAME"],
        )
        return conn

    pool = create_async_engine(
        "postgresql+asyncpg://",
        async_creator=getconn,
    )
    return pool


async def create_and_run_procedure(Session):
    procedure_sql = """
    CREATE OR REPLACE PROCEDURE create_customer_360()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- 1. Creating table level transformations

        -- a. Basic Customer Info
        CREATE TEMPORARY TABLE temp_basic_info AS
        SELECT
            customer_id,
            first_name,
            last_name,
            email,
            phone_number,
            date_of_birth,
            registration_date
        FROM customer_info;

        -- b. Purchase Statistics
        CREATE TEMPORARY TABLE temp_purchase_stats AS
        SELECT
            customer_id,
            COALESCE(SUM(total_amount), 0) AS total_lifetime_value,
            COUNT(DISTINCT transaction_id) AS total_purchases,
            MAX(purchase_date) AS last_purchase_date,
            COALESCE(AVG(total_amount), 0) AS average_order_value
        FROM purchase_transactions
        GROUP BY customer_id;

        -- c. Favorite Products and Brands
        CREATE TEMPORARY TABLE temp_favorites AS
        SELECT
            pt.customer_id,
            (
                SELECT pc.category
                FROM purchase_transactions pt2
                JOIN product_catalog pc ON pt2.product_id = pc.product_id
                WHERE pt2.customer_id = pt.customer_id
                GROUP BY pc.category
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS favorite_product_category,
            (
                SELECT pc.brand
                FROM purchase_transactions pt2
                JOIN product_catalog pc ON pt2.product_id = pc.product_id
                WHERE pt2.customer_id = pt.customer_id
                GROUP BY pc.brand
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS favorite_brand
        FROM purchase_transactions pt
        GROUP BY pt.customer_id;

        -- d. Customer Service Information
        CREATE TEMPORARY TABLE temp_customer_service AS
        SELECT
            customer_id,
            MAX(interaction_date) AS last_interaction_date,
            (
                SELECT interaction_type
                FROM customer_service cs2
                WHERE cs2.customer_id = cs.customer_id
                ORDER BY interaction_date DESC
                LIMIT 1
            ) AS last_interaction_type,
            AVG(satisfaction_score) AS average_satisfaction_score
        FROM customer_service cs
        GROUP BY customer_id;

        -- e. Website Behavior
        CREATE TEMPORARY TABLE temp_website_behavior AS
        SELECT
            customer_id,
            COUNT(DISTINCT session_id) AS total_website_visits,
            AVG(time_spent) AS average_time_spent_on_site,
            (
                SELECT pc.category
                FROM website_behavior wb2
                JOIN product_catalog pc ON wb2.pages_viewed = pc.product_id
                WHERE wb2.customer_id = wb.customer_id
                GROUP BY pc.category
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS most_viewed_product_category
        FROM website_behavior wb
        GROUP BY customer_id;

        -- f. Campaign Response Information
        CREATE TEMPORARY TABLE temp_campaign_response AS
        SELECT
            cr.customer_id,
            COALESCE(
                CAST(SUM(CASE WHEN cr.response_type IN ('click', 'purchase') THEN 1 ELSE 0 END) AS FLOAT) /
                NULLIF(COUNT(DISTINCT cr.campaign_id), 0),
                0
            ) AS campaign_response_rate,
            (
                SELECT mc.channel
                FROM campaign_responses cr2
                JOIN marketing_campaigns mc ON cr2.campaign_id = mc.campaign_id
                WHERE cr2.customer_id = cr.customer_id
                GROUP BY mc.channel
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS preferred_marketing_channel
        FROM campaign_responses cr
        GROUP BY cr.customer_id;

        -- 2. Joining subsets of the tables

        -- a. Customer Purchase Profile
        CREATE TEMPORARY TABLE temp_customer_purchase_profile AS
        SELECT
            bi.*,
            ps.total_lifetime_value,
            ps.total_purchases,
            ps.last_purchase_date,
            ps.average_order_value,
            f.favorite_product_category,
            f.favorite_brand
        FROM temp_basic_info bi
        LEFT JOIN temp_purchase_stats ps ON bi.customer_id = ps.customer_id
        LEFT JOIN temp_favorites f ON bi.customer_id = f.customer_id;

        -- b. Customer Engagement Profile
        CREATE TEMPORARY TABLE temp_customer_engagement_profile AS
        SELECT
            cs.customer_id,
            cs.last_interaction_date,
            cs.last_interaction_type,
            cs.average_satisfaction_score,
            wb.total_website_visits,
            wb.average_time_spent_on_site,
            wb.most_viewed_product_category
        FROM temp_customer_service cs
        LEFT JOIN temp_website_behavior wb ON cs.customer_id = wb.customer_id;

        -- 3. Joining the joined tables
        CREATE TEMPORARY TABLE temp_comprehensive_customer_profile AS
        SELECT
            cpp.*,
            cep.last_interaction_date,
            cep.last_interaction_type,
            cep.average_satisfaction_score,
            cep.total_website_visits,
            cep.average_time_spent_on_site,
            cep.most_viewed_product_category,
            cr.campaign_response_rate,
            cr.preferred_marketing_channel
        FROM temp_customer_purchase_profile cpp
        LEFT JOIN temp_customer_engagement_profile cep ON cpp.customer_id = cep.customer_id
        LEFT JOIN temp_campaign_response cr ON cpp.customer_id = cr.customer_id;

        -- 4. Creating the final table
        DROP TABLE IF EXISTS customer_360;
        CREATE TABLE customer_360 AS
        SELECT
            ccp.*,
            CASE
                WHEN ccp.total_lifetime_value > 1000 THEN 'High Value'
                WHEN ccp.total_lifetime_value > 500 THEN 'Medium Value'
                ELSE 'Low Value'
            END AS customer_segment,
            CURRENT_DATE - ccp.last_purchase_date AS recency_score,
            ccp.total_purchases AS frequency_score,
            ccp.total_lifetime_value AS monetary_score,
            CASE
                WHEN (CURRENT_DATE - ccp.last_purchase_date) > 180 THEN 'High'
                WHEN (CURRENT_DATE - ccp.last_purchase_date) > 90 THEN 'Medium'
                ELSE 'Low'
            END AS churn_risk_score
        FROM temp_comprehensive_customer_profile ccp;

        -- Clean up temporary tables
        DROP TABLE temp_basic_info, temp_purchase_stats, temp_favorites, temp_customer_service,
                   temp_website_behavior, temp_campaign_response, temp_customer_purchase_profile,
                   temp_customer_engagement_profile, temp_comprehensive_customer_profile;

    END;
    $$;
    """

    async with Session() as session:
        try:
            # Create the procedure
            await session.execute(text(procedure_sql))
            await session.commit()
            print("Procedure created successfully.")

            # Execute the procedure
            await session.execute(text("CALL create_customer_360()"))
            await session.commit()
            print("Procedure executed successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")
            await session.rollback()


async def main():
    print("Trying to connect...")

    connector = await create_async_connector()
    engine = await init_connection_pool(connector)
    Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    try:
        print("Connection established.")
        await create_and_run_procedure(Session)

    except Exception as e:
        print(f"Unable to establish connection: {e}")

    finally:
        if engine:
            await engine.dispose()
            print("Connection pool disposed.")
        await connector.close_async()


if __name__ == "__main__":
    asyncio.run(main())
