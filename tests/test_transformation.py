"""Unit tests for silver-layer transformation logic.

These tests use a local Spark session and small in-memory datasets so
they run anywhere — no Databricks cluster required.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from global_retail.constants import (
    BRONZE_CUSTOMER_TABLE,
    BRONZE_DB,
    BRONZE_PRODUCT_TABLE,
    BRONZE_TRANSACTION_TABLE,
)
from silver import customer_transform, order_transform, product_transform


@pytest.fixture(scope="session", name="spark")
def spark_fixture() -> SparkSession:
    """Provide a local Delta-enabled Spark session for the test session."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("global-retail-tests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def _seed_bronze_customers(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_DB}.{BRONZE_CUSTOMER_TABLE}")
    df = spark.createDataFrame(
        [
            (
                "1",
                "Alice",
                "a@x.com",
                "FR",
                "Premium",
                "2020-01-01",
                30,
                "F",
                12000,
            ),
            (
                "2",
                "Bob",
                None,
                "FR",
                "Regular",
                "2021-01-01",
                40,
                "M",
                6000,
            ),  # null email -> filtered
            (
                "3",
                "Eve",
                "e@x.com",
                "DE",
                "Regular",
                "2022-01-01",
                17,
                "F",
                100,
            ),  # underage -> filtered
            (
                "4",
                "Lo",
                "l@x.com",
                "IT",
                "Regular",
                "2019-06-01",
                28,
                "M",
                200,
            ),
        ],
        schema=(
            "customer_id string, name string, email string, "
            "country string, customer_type string, "
            "registration_date string, age int, gender string, "
            "total_purchases int"
        ),
    )
    from pyspark.sql.functions import current_timestamp, to_date

    df = df.withColumn(
        "registration_date", to_date("registration_date")
    ).withColumn("ingestion_timestamp", current_timestamp())
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.{BRONZE_CUSTOMER_TABLE}"
    )


def test_customer_silver_filters_and_segments(spark: SparkSession) -> None:
    """Customers with bad email or invalid age are removed; segments OK."""
    _seed_bronze_customers(spark)
    customer_transform.run()

    rows = spark.sql(
        "SELECT customer_id, customer_segment "
        "FROM globalretail_silver.silver_customers "
        "ORDER BY customer_id"
    ).collect()

    ids = [r["customer_id"] for r in rows]
    assert ids == ["1", "4"]
    segments = {r["customer_id"]: r["customer_segment"] for r in rows}
    assert segments["1"] == "High Value"
    assert segments["4"] == "Low Value"


def test_product_price_category(spark: SparkSession) -> None:
    """Price categorization buckets work as designed."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_DB}.{BRONZE_PRODUCT_TABLE}")
    from pyspark.sql.functions import current_timestamp

    df = spark.createDataFrame(
        [
            ("1", "P1", "Toys", "B", 50.0, 100, 4.0, True),
            ("2", "P2", "Tech", "B", 500.0, 5, 4.5, True),
            ("3", "P3", "Luxury", "B", 1500.0, 0, 4.8, True),
        ],
        schema=(
            "product_id string, name string, category string, "
            "brand string, price double, stock_quantity int, "
            "rating double, is_active boolean"
        ),
    ).withColumn("ingestion_timestamp", current_timestamp())
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.{BRONZE_PRODUCT_TABLE}"
    )

    product_transform.run()
    rows = {
        r["product_id"]: (r["price_category"], r["stock_status"])
        for r in spark.sql(
            "SELECT product_id, price_category, stock_status "
            "FROM globalretail_silver.silver_products"
        ).collect()
    }
    assert rows["1"] == ("Budget", "Sufficient Stock")
    assert rows["2"] == ("Standard", "Low Stock")
    assert rows["3"] == ("Premium", "Out of Stock")


def test_order_status_marks_zero_quantity_cancelled(
    spark: SparkSession,
) -> None:
    """Zero-quantity or zero-amount orders should be marked Cancelled."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_DB}.{BRONZE_TRANSACTION_TABLE}")
    from pyspark.sql.functions import current_timestamp

    df = spark.createDataFrame(
        [
            ("T1", "1", "1", 2, 100.0, "2024-01-01", "Card", "Online"),
            ("T2", "1", "2", 0, 0.0, "2024-01-02", "Card", "Online"),
        ],
        schema=(
            "transaction_id string, customer_id string, "
            "product_id string, quantity int, total_amount double, "
            "transaction_date string, payment_method string, "
            "store_type string"
        ),
    ).withColumn("ingestion_timestamp", current_timestamp())
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.{BRONZE_TRANSACTION_TABLE}"
    )

    order_transform.run()
    rows = {
        r["transaction_id"]: r["order_status"]
        for r in spark.sql(
            "SELECT transaction_id, order_status "
            "FROM globalretail_silver.silver_orders"
        ).collect()
    }
    assert rows["T1"] == "Completed"
    assert rows["T2"] == "Cancelled"
