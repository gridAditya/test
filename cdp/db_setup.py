import os, random
import asyncio, asyncpg

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from asyncpg import Connection

from google.cloud.sql.connector import Connector, create_async_connector

from typing import List, Dict, Any
from colorama import Fore, Style
from datetime import timedelta
from dotenv import load_dotenv
from faker import Faker
from db_setup_queries import table_schema_init_queries

load_dotenv()  # load environment variables

# Below code involves parallel transactions with batch processing(size=500)[see bulk_insert() and insert_fake() functions] to speed up insertion of fake data
# Synchronous code takes more than 2 hours while async version takes only 1min for inserting data into `Cloud SQL` platform of `Google Cloud`


async def init_connection_pool(connector: Connector) -> AsyncEngine:
    async def getconn() -> Connection:
        conn: asyncpg.Connection = await connector.connect_async(
            os.environ[
                "INSTANCE_CONNECTION_NAME"
            ],  # Cloud SQL instance connection name
            "asyncpg",
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            db=os.environ["DB_NAME"],
        )
        return conn

    pool = create_async_engine(
        "postgresql+asyncpg://",  # Asyncpg driver
        async_creator=getconn,  # Use async connection creator
    )
    return pool


async def initiate_schema(Session, table_schema_init_queries: dict):
    try:
        async with Session() as session:
            # Iterate over the schema creation queries
            for table_name, create_query in table_schema_init_queries.items():
                try:
                    print(
                        f"{Fore.BLUE}{Style.BRIGHT}[+] Creating table {table_name}...{Style.RESET_ALL}"
                    )

                    # Execute the CREATE TABLE statement
                    await session.execute(text(create_query))

                    # Check if the table was created successfully by querying the information schema
                    result = await session.execute(
                        text(
                            f"SELECT * FROM information_schema.tables WHERE table_name='{table_name}';"
                        )
                    )
                    result = result.fetchall()

                    if result:
                        print(
                            f"{Fore.BLUE}{Style.BRIGHT}[+] Table {table_name} created successfully...{Style.RESET_ALL}\n"
                        )
                    else:
                        print(
                            f"{Fore.RED}{Style.BRIGHT}[-] Failed to create table {table_name}, no table found in information_schema.{Style.RESET_ALL}\n"
                        )

                except Exception as query_error:
                    print(
                        f"{Fore.RED}{Style.BRIGHT}[-] Error creating table {table_name}: {query_error}{Style.RESET_ALL}\n"
                    )

            # Commit the transaction after all tables have been processed
            await session.commit()  # Commit the transaction to persist changes

            print(
                f"{Fore.GREEN}{Style.BRIGHT}[+] Database Schema initialization process completed.{Style.RESET_ALL}\n",
                end="\n\n",
            )
            return True

    except Exception as e:
        print(
            f"{Fore.RED}{Style.BRIGHT}[-] An error occurred while creating tables: {e}{Style.RESET_ALL}",
            end="\n\n",
        )
    return False


async def bulk_insert(
    session: AsyncSession, table_name: str, data: list, batch_size: int = 500
):
    try:
        columns = list(data[0].keys())
        placeholders = ", ".join(
            [f":{col}" for col in columns]
        )  # SQLAlchemy style placeholders
        column_names = ", ".join(columns)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        # Insert data in batches asynchronously
        for i in range(0, len(data), batch_size):
            batch_data = data[i : i + batch_size]
            await session.execute(
                text(query), batch_data
            )  # Execute the query using the session
            print(
                f"{Fore.BLUE}{Style.BRIGHT}Successfully inserted {len(batch_data)} rows into {table_name}{Style.RESET_ALL}"
            )

    except Exception as e:
        print(
            f"{Fore.RED}{Style.BRIGHT}Error inserting data into {table_name}: {str(e)}{Style.RESET_ALL}"
        )
        raise  # Rethrow the exception to handle rollback in the calling function


async def insert_fake_data(Session: AsyncSession):
    fake = Faker()

    # Prepare data for bulk inserts
    num_customer, num_products, num_campaigns = 1000, 100, 20

    customer_data = prepare_customer_data(fake, num_customer)
    product_data = prepare_product_data(fake, num_products)
    purchase_data = prepare_purchase_data(fake, 5000, num_customer, num_products)
    service_data = prepare_service_data(fake, 2000, num_customer, num_products)
    campaign_data = prepare_campaign_data(fake, num_campaigns)
    response_data = prepare_response_data(fake, 10000, num_customer, num_campaigns)
    behavior_data = prepare_behavior_data(fake, 20000, num_customer)

    async with Session() as session:
        try:
            # Step 1: Insert parent table data(Parent in Foreign Key relation) (customer_info, product_catalog, marketing_campaigns) in parallel
            await asyncio.gather(
                bulk_insert(session, "customer_info", customer_data),
                bulk_insert(session, "product_catalog", product_data),
                bulk_insert(session, "marketing_campaigns", campaign_data),
            )
            await session.commit()  # Commit the transaction after the first stage of inserts

            # Step 2: Insert child table data(Child in Foreign Key relation) (purchase_transactions, customer_service, campaign_responses, website_behavior) in parallel
            await asyncio.gather(
                bulk_insert(session, "purchase_transactions", purchase_data),
                bulk_insert(session, "customer_service", service_data),
                bulk_insert(session, "campaign_responses", response_data),
                bulk_insert(session, "website_behavior", behavior_data),
            )
            await session.commit()  # Commit the transaction after the second stage of inserts
            return True
        except Exception as e:
            await session.rollback()  # Rollback if an error occurs
            print(
                f"{Fore.RED}{Style.BRIGHT}[-] An error occurred while inserting data: {e}{Style.RESET_ALL}",
                end="\n\n",
            )
            print(e)
            return False


def prepare_customer_data(fake: Faker, count: int) -> List[Dict[str, Any]]:
    return [
        {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "registration_date": fake.date_between(start_date="-5y", end_date="today"),
        }
        for _ in range(count)
    ]


def prepare_product_data(fake: Faker, count: int) -> List[Dict[str, Any]]:
    categories = ["Home Care", "Personal Care", "Baby Care", "Fabric Care", "Hair Care"]
    brands = [
        "Tide",
        "Pampers",
        "Gillette",
        "Pantene",
        "Oral-B",
        "Olay",
        "Dawn",
        "Bounty",
        "Charmin",
        "Crest",
    ]
    return [
        {
            "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "price": round(random.uniform(5, 100), 2),
            "launch_date": fake.date_between(start_date="-3y", end_date="today"),
        }
        for _ in range(count)
    ]


def prepare_purchase_data(
    fake: Faker, count: int, num_customer: int, num_products: int
) -> List[Dict[str, Any]]:
    return [
        {
            "customer_id": random.randint(1, num_customer),
            "product_id": random.randint(1, num_products),
            "purchase_date": fake.date_between(start_date="-1y", end_date="today"),
            "quantity": random.randint(1, 5),
            "total_amount": round(random.uniform(10, 500), 2),
            "store_id": random.randint(1, 50),
        }
        for _ in range(count)
    ]


def prepare_service_data(
    fake: Faker, count: int, num_customer: int, num_products: int
) -> List[Dict[str, Any]]:
    interaction_types = ["complaint", "inquiry", "feedback"]
    resolution_statuses = ["resolved", "pending", "escalated"]

    return [
        {
            "customer_id": random.randint(1, num_customer),
            "interaction_date": fake.date_between(start_date="-1y", end_date="today"),
            "interaction_type": random.choice(interaction_types),
            "product_id": random.randint(1, num_products),
            "resolution_status": random.choice(resolution_statuses),
            "satisfaction_score": random.randint(1, 10),
        }
        for _ in range(count)
    ]


def prepare_campaign_data(fake: Faker, count: int) -> List[Dict[str, Any]]:
    channels = ["email", "social media", "TV", "print", "radio"]
    target_audiences = ["all", "young adults", "parents", "seniors"]

    campaigns = []
    for _ in range(count):
        start_date = fake.date_between(start_date="-6m", end_date="today")
        end_date = start_date + timedelta(days=random.randint(7, 90))

        campaign = {
            "campaign_name": fake.catch_phrase(),
            "start_date": start_date,
            "end_date": end_date,
            "channel": random.choice(channels),
            "target_audience": random.choice(target_audiences),
        }
        campaigns.append(campaign)

    return campaigns


def prepare_response_data(
    fake: Faker, count: int, num_customer: int, num_campaigns: int
) -> List[Dict[str, Any]]:
    response_types = ["click", "purchase", "unsubscribe"]

    return [
        {
            "campaign_id": random.randint(1, num_campaigns),
            "customer_id": random.randint(1, num_customer),
            "response_date": fake.date_between(start_date="-6m", end_date="today"),
            "response_type": random.choice(response_types),
        }
        for _ in range(count)
    ]


def prepare_behavior_data(
    fake: Faker, count: int, num_customer: int
) -> List[Dict[str, Any]]:
    sources = ["organic search", "paid ad", "direct", "social media", "email"]

    return [
        {
            "customer_id": random.randint(1, num_customer),
            "visit_date": fake.date_between(start_date="-1y", end_date="today"),
            "pages_viewed": random.randint(1, 20),
            "time_spent": random.randint(30, 1800),
            "source": random.choice(sources),
        }
        for _ in range(count)
    ]


async def main():
    print(
        f"{Fore.GREEN}{Style.BRIGHT}[+] Trying to connect....{Style.RESET_ALL}",
        end="\n\n",
    )

    connector = await create_async_connector()  # Initialize Cloud SQL Connector
    engine = await init_connection_pool(connector)  # Initialize connection pool
    Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)  # session manager

    try:
        print(
            f"{Fore.GREEN}{Style.BRIGHT}[+] Connection established.{Style.RESET_ALL}",
            end="\n\n",
        )
        db_init = await initiate_schema(Session, table_schema_init_queries)

        print(
            f"{Fore.GREEN}{Style.BRIGHT}[+] Begginning data insertion into db.{Style.RESET_ALL}",
            end="\n\n",
        )
        try:
            result = await insert_fake_data(Session)
            if result:
                print(
                    f"{Fore.GREEN}{Style.BRIGHT}[+] Database initialised successfully with data.{Style.RESET_ALL}",
                    end="\n\n",
                )

        except Exception as e:
            print(
                f"{Fore.RED}{Style.BRIGHT}[-] Unable to insert into db.{Style.RESET_ALL}"
            )
            print(e, end="\n\n")

    except Exception as e:
        print(
            f"{Fore.RED}{Style.BRIGHT}[-] Unable to establish connection.{Style.RESET_ALL}",
            end="\n\n",
        )
        print(e)

    finally:
        # Ensure the connection pool is disposed
        if engine:
            await engine.dispose()
            print(
                f"{Fore.GREEN}{Style.BRIGHT}Connection pool disposed.{Style.RESET_ALL}"
            )
        await connector.close_async()  # Ensure the Cloud SQL Connector is properly closed


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
