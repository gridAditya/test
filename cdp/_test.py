import os
import cdp_procedure
import pg8000
import sqlalchemy
import pytest
from google.cloud.sql.connector import Connector, IPTypes
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import text
from colorama import Fore, Style
from dotenv import load_dotenv

load_dotenv()  # Load environment variables

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]
    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return pool

@pytest.fixture(scope="module")
def db_session():
    # Create the SQLAlchemy engine and session factory
    print(f"{Fore.GREEN}{Style.BRIGHT}[+] Trying to connect....{Style.RESET_ALL}", end="\n\n")
    engine = connect_with_connector()
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    print(f"{Fore.GREEN}{Style.BRIGHT}[+] Connection established.{Style.RESET_ALL}", end="\n\n")

    session = Session()  # Create a session instance
    yield session  # Yield the session instance to the tests
    
    session.close()  # Teardown: close the session after the tests are completed
    print(f"{Fore.GREEN}{Style.BRIGHT}[+] Session closed.{Style.RESET_ALL}", end="\n\n")

def test_unique_customers(db_session):
    """
    Test that the number of customers remains consistent before and after transformation.
    """
    source_customer_count = "SELECT COUNT(DISTINCT(customer_id)) from customer_info;"
    customer_360_count = "SELECT COUNT(DISTINCT(customer_id)) from customer_360;"

    with db_session as s:  # Use the session instance provided by the fixture
        source_result = s.execute(text(source_customer_count)).scalar()
        customer_360_result = s.execute(text(customer_360_count)).scalar()

    assert source_result == customer_360_result, (
        f"The number of unique customers does not match. "
        f"Source Table Count: {source_result}, Customer 360 Table Count: {customer_360_result}"
    )

def test_total_money(db_session):
    """
    Test that the total amount of money spent by all customers remains consistent before and after transformation.
    We actually check if they are within a small tolerance(50), since due to floating point precision exact match can't
    be obtained.
    """
    source_purchase_amount = "SELECT SUM(total_amount) from purchase_transactions;"
    customer_360_amount = "SELECT SUM(total_lifetime_value) from customer_360;"

    with db_session as s:
        source_result = s.execute(text(source_purchase_amount)).scalar()
        customer_360_result = s.execute(text(customer_360_amount)).scalar()

    assert abs(source_result - customer_360_result) <= 50, (
        f"Total amount of money spent before and after transformation does not match. "
        f"Total Money Before Transformation: {source_result}, Total Money After Transformation: {customer_360_result}"
    )

def test_total_spending_per_customer(db_session):
    """
    Tests whether the amount of spending per customer remains same before and after transformation
    """
    query = """
        SELECT pt.customer_id, SUM(pt.total_amount) AS actual_total_amount, 
        c360.total_lifetime_value AS aggregated_total_lifetime_value
        FROM purchase_transactions pt
        JOIN customer_360 c360 ON pt.customer_id = c360.customer_id
        GROUP BY pt.customer_id, c360.total_lifetime_value
        HAVING SUM(pt.total_amount) <> c360.total_lifetime_value;
    """

    with db_session as s:
        result = s.execute(text(query)).fetchall()

    assert len(result) == 0, "Total Spending per customer is different before and after the transformation"

def test_total_visits(db_session):
    """
    Test whether the total number of visits made by valid customers to the CDP remain consistent before and after the transformation.
    Valid customers are those whose `customer_id` is also present in `customer_service` table, i.e the customers who actually
    had a complaint, filed it and whose complaint resolution was tracked. Only these customers would visit the website
    and have their session_ids tracked.
    """
    source_total_visits = """
        SELECT COUNT(wb.session_id)
        FROM website_behavior wb
        WHERE wb.customer_id IN (SELECT DISTINCT cs.customer_id FROM customer_service cs);
    """
    customer_360_total_visits = "SELECT SUM(total_website_visits) from customer_360;"

    with db_session as s:
        source_result = s.execute(text(source_total_visits)).scalar()
        customer_360_result = s.execute(text(customer_360_total_visits)).scalar()

    assert source_result == customer_360_result, (
        f"Total number of visits made to CDP do not match. "
        f"Total Visits Before Transformation: {source_result}, Total Visits After Transformation: {customer_360_result}"
    )

def test_total_purchases(db_session):
    """
    Test whether the total number of purchases made by customers remains consistent before and after the transformation
    """
    source_total_purchases = "SELECT COUNT(DISTINCT(transaction_id)) from purchase_transactions;"
    customer_360_total_purchases = "SELECT SUM(total_purchases) from customer_360;"

    with db_session as s:
        source_result = s.execute(text(source_total_purchases)).scalar()
        customer_360_result = s.execute(text(customer_360_total_purchases)).scalar()

    assert source_result == customer_360_result, (
        f"The total number of purchases made by customers do not match. "
        f"Total Purchases Before Transformation: {source_result}, Total Purchases After Transformation: {customer_360_result}"
    )

def test_frequency_score(db_session):
    """
    Test that the freuqency score of each customer remains consistent before and after the transformation
    """
    query = """
        SELECT pt.customer_id, 
        COUNT(DISTINCT pt.transaction_id) AS actual_total_purchases, 
        c360.total_purchases AS aggregated_frequency_score
        FROM purchase_transactions pt
        JOIN customer_360 c360 ON pt.customer_id = c360.customer_id
        GROUP BY pt.customer_id, c360.total_purchases
        HAVING COUNT(DISTINCT pt.transaction_id) <> c360.total_purchases;
    """

    with db_session as s:
        result = s.execute(text(query)).fetchall()

    assert len(result) == 0, "Frequency score for each customer is not consistent before and after the transformation"
