"""Data-quality assertions against the Silver layer (Pipeline A).

Requires env vars SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_WAREHOUSE (role: TRANSFORM_ROLE or higher). Skipped automatically
if credentials aren't present, so lint-only CI runs don't fail.
"""

import os

import pytest

snowflake_connector = pytest.importorskip("snowflake.connector")

REQUIRED_ENV = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE"]
pytestmark = pytest.mark.skipif(
    any(os.environ.get(var) is None for var in REQUIRED_ENV),
    reason="Snowflake credentials not configured",
)


@pytest.fixture(scope="module")
def conn():
    connection = snowflake_connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="RETAIL_DWH",
        schema="SILVER",
    )
    yield connection
    connection.close()


def scalar(conn, query: str):
    cur = conn.cursor()
    try:
        return cur.execute(query).fetchone()[0]
    finally:
        cur.close()


def test_clean_orders_has_no_null_order_id(conn):
    assert scalar(conn, "SELECT COUNT(*) FROM CLEAN_ORDERS WHERE ORDER_ID IS NULL") == 0


def test_clean_orders_has_no_negative_amounts(conn):
    assert scalar(conn, "SELECT COUNT(*) FROM CLEAN_ORDERS WHERE AMOUNT < 0") == 0


def test_clean_orders_order_id_is_unique(conn):
    total = scalar(conn, "SELECT COUNT(*) FROM CLEAN_ORDERS")
    distinct = scalar(conn, "SELECT COUNT(DISTINCT ORDER_ID) FROM CLEAN_ORDERS")
    assert total == distinct


def test_clean_orders_status_is_known_value(conn):
    known = (
        "'pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded'"
    )
    unknown_count = scalar(
        conn, f"SELECT COUNT(*) FROM CLEAN_ORDERS WHERE STATUS NOT IN ({known})"
    )
    assert unknown_count == 0


def test_every_order_has_a_known_customer(conn):
    orphans = scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM CLEAN_ORDERS o
        LEFT JOIN CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
        WHERE c.CUSTOMER_ID IS NULL
        """,
    )
    assert orphans == 0
