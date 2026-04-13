"""Silver layer transformation for customers.

Cleans, validates, and enriches the bronze customer table, then merges
the result into the silver customer Delta table using an incremental
watermark-driven pattern.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from global_retail.constants import (
    BRONZE_CUSTOMER_TABLE,
    BRONZE_DB,
    SILVER_CUSTOMER_TABLE,
    SILVER_DB,
)
from global_retail.utils import (
    ensure_database,
    get_last_watermark,
    get_logger,
    get_spark,
)

LOGGER = get_logger(__name__)

CREATE_SILVER_CUSTOMERS_SQL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_CUSTOMER_TABLE} (
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP
) USING DELTA
"""


def create_silver_table(spark: SparkSession) -> None:
    """Create the silver customer table if it does not yet exist."""
    ensure_database(spark, SILVER_DB)
    spark.sql(CREATE_SILVER_CUSTOMERS_SQL)


def build_incremental_view(spark: SparkSession, watermark: str) -> None:
    """Create a temp view of cleaned customers newer than ``watermark``.

    Cleaning rules:
        * email must not be null
        * age must be between 18 and 100 (inclusive)
        * total_purchases must be non-negative

    Enrichment:
        * customer_segment derived from total_purchases
        * days_since_registration computed from registration_date
    """
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW silver_incremental_customers AS
        SELECT
            customer_id,
            name,
            email,
            country,
            customer_type,
            registration_date,
            age,
            gender,
            total_purchases,
            CASE
                WHEN total_purchases > 10000 THEN 'High Value'
                WHEN total_purchases > 5000  THEN 'Medium Value'
                ELSE 'Low Value'
            END AS customer_segment,
            DATEDIFF(CURRENT_DATE(), registration_date)
                AS days_since_registration,
            CURRENT_TIMESTAMP() AS last_updated
        FROM {BRONZE_DB}.{BRONZE_CUSTOMER_TABLE}
        WHERE ingestion_timestamp > '{watermark}'
          AND email IS NOT NULL
          AND age BETWEEN 18 AND 100
          AND total_purchases >= 0
        """)


def merge_into_silver(spark: SparkSession) -> None:
    """Merge the incremental view into the silver customer table."""
    spark.sql(f"""
        MERGE INTO {SILVER_DB}.{SILVER_CUSTOMER_TABLE} AS target
        USING silver_incremental_customers AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)


def run() -> None:
    """Execute the customer silver transformation job."""
    spark = get_spark()
    create_silver_table(spark)

    watermark = get_last_watermark(
        spark, f"{SILVER_DB}.{SILVER_CUSTOMER_TABLE}"
    )
    LOGGER.info("Customer silver watermark: %s", watermark)

    build_incremental_view(spark, watermark)
    merge_into_silver(spark)
    LOGGER.info("Silver customer merge complete.")


if __name__ == "__main__":
    run()
