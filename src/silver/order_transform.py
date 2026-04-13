"""Silver layer transformation for orders (transactions)."""

from __future__ import annotations

from pyspark.sql import SparkSession

from global_retail.constants import (
    BRONZE_DB,
    BRONZE_TRANSACTION_TABLE,
    SILVER_DB,
    SILVER_ORDER_TABLE,
)
from global_retail.utils import (
    ensure_database,
    get_last_watermark,
    get_logger,
    get_spark,
)

LOGGER = get_logger(__name__)

CREATE_SILVER_ORDERS_SQL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_ORDER_TABLE} (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    payment_method STRING,
    store_type STRING,
    order_status STRING,
    last_updated TIMESTAMP
) USING DELTA
"""


def create_silver_table(spark: SparkSession) -> None:
    """Create the silver order table if it does not yet exist."""
    ensure_database(spark, SILVER_DB)
    spark.sql(CREATE_SILVER_ORDERS_SQL)


def build_incremental_view(
    spark: SparkSession, watermark: str
) -> None:
    """Build a cleaned, enriched temp view of new orders."""
    spark.sql(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW silver_incremental_orders AS
        SELECT
            transaction_id,
            CAST(customer_id AS STRING) AS customer_id,
            CAST(product_id  AS STRING) AS product_id,
            CASE WHEN quantity     < 0 THEN 0 ELSE quantity     END
                AS quantity,
            CASE WHEN total_amount < 0 THEN 0 ELSE total_amount END
                AS total_amount,
            CAST(transaction_date AS DATE) AS transaction_date,
            payment_method,
            store_type,
            CASE
                WHEN quantity = 0 OR total_amount = 0 THEN 'Cancelled'
                ELSE 'Completed'
            END AS order_status,
            CURRENT_TIMESTAMP() AS last_updated
        FROM {BRONZE_DB}.{BRONZE_TRANSACTION_TABLE}
        WHERE ingestion_timestamp > '{watermark}'
          AND transaction_date IS NOT NULL
          AND customer_id      IS NOT NULL
          AND product_id       IS NOT NULL
        """
    )


def merge_into_silver(spark: SparkSession) -> None:
    """Merge the incremental view into the silver order table."""
    spark.sql(
        f"""
        MERGE INTO {SILVER_DB}.{SILVER_ORDER_TABLE} AS target
        USING silver_incremental_orders AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def run() -> None:
    """Execute the order silver transformation job."""
    spark = get_spark()
    create_silver_table(spark)

    watermark = get_last_watermark(
        spark, f"{SILVER_DB}.{SILVER_ORDER_TABLE}"
    )
    LOGGER.info("Order silver watermark: %s", watermark)

    build_incremental_view(spark, watermark)
    merge_into_silver(spark)
    LOGGER.info("Silver order merge complete.")


if __name__ == "__main__":
    run()